"""Opencode HTTP客户端 -- 基于代理池，无需API密钥。"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Union

import aiohttp

from src.core.dispatch.candidate import Candidate, make_id
from src.core.utils.errors import PlatformError
from src.foundation.logger import get_logger
from .constants import (
    BASE_URL,
    CAPS,
    CHAT_PATH,
    FILTER_PAID_MODELS,
    MAX_RETRIES,
    MODELS,
    MODELS_PATH,
    PROXY_FETCH_ENABLED,
    PROXY_POOL_PERSIST_PATH,
    PROXY_REFRESH_INTERVAL,
    PROXY_SCORE_PERSIST_PATH,
)
from .utils import build_headers, build_payload, parse_sse_line
from .proxypool import ProxyInfo, ProxyPool, fetch_all_proxies
from .proxyscore import DIRECT, ProxyPoolSelector

from ..config_seed import load_local_proxies

LOCAL_PROXIES: list = load_local_proxies()

logger = get_logger(__name__)


class OpencodeClient:
    """Opencode HTTP客户端。

    不使用API密钥，而是将代理池中的每个代理视为候选节点。
    通过ProxyPoolSelector（基于汤普森采样算法）为每个请求选择最优代理，
    代理池定期刷新，启动时优先使用有效缓存。

    PROXY_FETCH_ENABLED=False 时：
    - 保留代理池缓存和评分数据（不丢失学习结果）
    - 请求时仅使用直连，不通过代理
    - 后台不刷新代理池
    """

    # 缓存有效期：超过此时间的缓存视为过期，需要刷新
    _POOL_CACHE_MAX_AGE: float = PROXY_REFRESH_INTERVAL * 0.8  # 80% 刷新间隔

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        self._pool: ProxyPool = ProxyPool()
        self._selector: ProxyPoolSelector = ProxyPoolSelector(PROXY_SCORE_PERSIST_PATH)
        self._models: List[str] = list(MODELS)
        self._refresh_task: Optional[asyncio.Task] = None
        self._proxy_lock: asyncio.Lock = asyncio.Lock()
        self._active_requests: int = 0
        self._pool_refresh_pending: bool = False
        self._last_fetch_time: float = 0.0  # 上次成功获取代理池的时间戳

    # ------------------------------------------------------------------
    # 初始化方法
    # ------------------------------------------------------------------

    async def init_immediate(self, session: aiohttp.ClientSession) -> None:
        """存储会话对象并加载缓存的代理池 -- 非阻塞操作。

        无论 PROXY_FETCH_ENABLED 是否为 True，都加载缓存代理池和评分数据。
        始终注入本地代理。如果缓存有效且 PROXY_FETCH_ENABLED=True，跳过后续刷新。
        """
        self._session = session

        # 始终加载缓存代理池（保留学习数据）
        pool = await self._load_pool_from_disk()
        if pool is not None and pool.count > 0:
            self._inject_local_proxies(pool)
            self._pool = pool
            self._selector.update_pool(pool.to_address_list())
            self._last_fetch_time = pool.fetch_time_epoch
            logger.debug(
                "opencode client init_immediate: loaded %d cached proxies (age=%.0fs, fetch_enabled=%s)",
                pool.count,
                time.time() - self._last_fetch_time if self._last_fetch_time else 0,
                PROXY_FETCH_ENABLED,
            )
        else:
            # 无缓存时至少注入本地代理
            local_pool = ProxyPool()
            self._inject_local_proxies(local_pool)
            if local_pool.count > 0:
                self._pool = local_pool
                self._selector.update_pool(local_pool.to_address_list())
            logger.debug(
                "opencode client init_immediate: no valid cache, %d local proxies (fetch_enabled=%s)",
                local_pool.count,
                PROXY_FETCH_ENABLED,
            )

    async def background_setup(self) -> None:
        """后台获取新代理池（在线程池中执行）并启动定期刷新任务。

        仅在 PROXY_FETCH_ENABLED=True 时执行刷新。
        PROXY_FETCH_ENABLED=False 时保留已有缓存，不触发任何网络请求。
        """
        if not PROXY_FETCH_ENABLED:
            logger.debug(
                "opencode background_setup: proxy fetch disabled, "
                "keeping cached pool (%d proxies) and score data intact",
                self._pool.count,
            )
            return

        # 检查缓存是否有效
        cache_age = time.time() - self._last_fetch_time if self._last_fetch_time else float("inf")
        cache_valid = (
            self._pool.count > 0
            and self._last_fetch_time > 0
            and cache_age < self._POOL_CACHE_MAX_AGE
        )

        if cache_valid:
            logger.debug(
                "opencode background_setup: cache valid (age=%.0fs < max=%.0fs), skipping immediate refresh",
                cache_age, self._POOL_CACHE_MAX_AGE,
            )
        else:
            logger.debug(
                "opencode background_setup: cache expired or empty (age=%.0fs), fetching now",
                cache_age if cache_age != float("inf") else -1,
            )
            try:
                loop = asyncio.get_running_loop()
                pool = await loop.run_in_executor(None, self._do_proxy_fetch)
                await self._apply_pool(pool)
            except Exception as e:
                logger.warning("opencode background proxy fetch failed: %s", e)

        # 启动定期刷新任务
        self._refresh_task = asyncio.ensure_future(self._bg_refresh_proxy())

    # ------------------------------------------------------------------
    # 代理池管理
    # ------------------------------------------------------------------

    @staticmethod
    def _do_proxy_fetch() -> ProxyPool:
        """同步获取代理（在线程池中调用）。"""
        return fetch_all_proxies()

    async def _apply_pool(self, pool: ProxyPool) -> None:
        """在锁保护下应用新获取的代理池，合并本地代理并持久化。

        记录获取时间戳，用于判断缓存有效期。
        """
        self._inject_local_proxies(pool)
        
        async with self._proxy_lock:
            if self._active_requests > 0:
                self._pool_refresh_pending = True
                logger.debug(
                    "Proxy pool refresh deferred: %d active requests, %d new proxies",
                    self._active_requests, pool.count,
                )
                return
            
            self._pool = pool
            self._selector.update_pool(pool.to_address_list())
            self._last_fetch_time = pool.fetch_time_epoch
            self._pool_refresh_pending = False
            await self._save_pool_to_disk(pool)
            logger.debug(
                "Proxy pool refreshed: %d proxies, fetch_time=%s (epoch=%.0f)",
                pool.count, pool.fetch_time, self._last_fetch_time,
            )

    async def _check_deferred_refresh(self) -> None:
        """检查并应用待处理的代理池刷新。"""
        if not PROXY_FETCH_ENABLED:
            return
        if self._pool_refresh_pending and self._active_requests == 0:
            try:
                loop = asyncio.get_running_loop()
                pool = await loop.run_in_executor(None, self._do_proxy_fetch)
                self._inject_local_proxies(pool)
                
                async with self._proxy_lock:
                    self._pool = pool
                    self._selector.update_pool(pool.to_address_list())
                    self._last_fetch_time = pool.fetch_time_epoch
                    self._pool_refresh_pending = False
                    await self._save_pool_to_disk(pool)
                    logger.debug(
                        "Deferred proxy pool refresh applied: %d proxies",
                        pool.count,
                    )
            except Exception as e:
                logger.warning("Failed to apply deferred proxy refresh: %s", e)

    @staticmethod
    def _inject_local_proxies(pool: ProxyPool) -> None:
        """将accounts.py中的本地代理合并到代理池中。"""
        for addr in LOCAL_PROXIES:
            addr = addr.strip()
            if not addr or ":" not in addr:
                continue
            parts = addr.rsplit(":", 1)
            try:
                ip, port = parts[0], int(parts[1])
            except (ValueError, IndexError):
                continue
            pool.add(ProxyInfo(
                ip=ip, port=port, protocol="http",
                country="local",
            ))

    async def _bg_refresh_proxy(self) -> None:
        """定期刷新代理池的后台任务。

        仅在 PROXY_FETCH_ENABLED=True 时运行。
        """
        if not PROXY_FETCH_ENABLED:
            return

        try:
            while True:
                await asyncio.sleep(PROXY_REFRESH_INTERVAL)
                try:
                    loop = asyncio.get_running_loop()
                    pool = await loop.run_in_executor(None, self._do_proxy_fetch)
                    await self._apply_pool(pool)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("opencode periodic proxy refresh failed: %s", e)
        except asyncio.CancelledError:
            raise

    async def _load_pool_from_disk(self) -> Optional[ProxyPool]:
        """从JSON文件反序列化缓存的代理池。

        无论 PROXY_FETCH_ENABLED 如何，都尝试加载。
        返回 None 如果文件不存在、损坏或缓存为空。
        """
        path = Path(PROXY_POOL_PERSIST_PATH)
        if not path.exists():
            logger.debug("No pool cache file at %s", path)
            return None
        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
            pool = ProxyPool.from_dict(data)
            if pool.count == 0:
                logger.debug("Loaded pool cache is empty")
                return None
            logger.debug(
                "Loaded pool cache: %d proxies, fetch_time=%s",
                pool.count, pool.fetch_time,
            )
            return pool
        except Exception as e:
            logger.warning("Failed to load proxy pool from %s: %s", path, e)
            return None

    async def _save_pool_to_disk(self, pool: ProxyPool) -> None:
        """原子性地将代理池持久化到JSON文件。

        无论 PROXY_FETCH_ENABLED 如何，都保存。
        """
        path = Path(PROXY_POOL_PERSIST_PATH)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(pool.to_dict(), indent=2), encoding="utf-8"
            )
            os.replace(str(tmp), str(path))
        except Exception as e:
            logger.warning("Failed to save proxy pool to %s: %s", path, e)

    # ------------------------------------------------------------------
    # 候选节点管理
    # ------------------------------------------------------------------

    async def candidates(self) -> List[Candidate]:
        """返回单个候选节点 -- 代理选择在内部处理。"""
        if self._pool.count == 0 and PROXY_FETCH_ENABLED:
            # 只有启用代理且池为空时才需要代理
            return []
        return [
            Candidate(
                id=make_id("opencode"),
                platform="opencode",
                resource_id="proxy-pool",
                models=list(self._models),
                context_length=None,
                meta={"proxy_addr": "", "proxy_protocol": ""},
                **CAPS,
            )
        ]

    async def ensure_candidates(self, count: int) -> int:
        """确保候选节点可用（代理池非空）。

        即使 PROXY_FETCH_ENABLED=False，只要有本地代理或缓存就返回可用。
        PROXY_FETCH_ENABLED=True 且池为空时尝试获取。
        """
        if self._pool.count == 0 and PROXY_FETCH_ENABLED:
            try:
                loop = asyncio.get_running_loop()
                pool = await loop.run_in_executor(None, self._do_proxy_fetch)
                await self._apply_pool(pool)
            except Exception as e:
                logger.warning("opencode ensure_candidates fetch failed: %s", e)
        return 1 if self._pool.count > 0 else 0

    # ------------------------------------------------------------------
    # 聊天补全
    # ------------------------------------------------------------------

    async def complete(
        self,
        candidate: Candidate,
        messages: List[Dict],
        model: str,
        stream: bool,
        *,
        thinking: bool = False,
        search: bool = False,
        **kw: Any,
    ) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
        """执行聊天补全请求，最多重试MAX_RETRIES次。

        PROXY_FETCH_ENABLED=True 时通过汤普森采样选择最优代理。
        PROXY_FETCH_ENABLED=False 时始终直连，但仍记录评分（保留学习数据）。
        """
        last_exc: Optional[Exception] = None
        failed_proxies: set = set()
        self._active_requests += 1
        
        try:
            attempt = 0
            while attempt < MAX_RETRIES:
                attempt += 1
                
                if PROXY_FETCH_ENABLED and self._pool.count > 0:
                    # 启用代理且有可用代理：汤普森采样选择
                    async with self._proxy_lock:
                        pool_addrs = self._pool.to_address_list()
                    
                    available_proxies = [
                        a for a in pool_addrs
                        if a not in failed_proxies
                    ]
                    
                    if not available_proxies:
                        logger.warning(
                            "No available proxies left for attempt %d/%d (%d failed proxies)",
                            attempt, MAX_RETRIES, len(failed_proxies),
                        )
                        raise PlatformError(
                            f"No available proxies in pool (tried {len(failed_proxies)} proxies)"
                        )
                    
                    chosen = self._selector.select(available_proxies)
                    if chosen == DIRECT or chosen is None:
                        new_addr = ""
                    else:
                        new_addr = chosen
                else:
                    # 未启用代理或池为空：直连
                    new_addr = ""
                    
                candidate.meta["proxy_addr"] = new_addr
                candidate.meta["proxy_protocol"] = "proxy" if new_addr else "direct"

                logger.debug(
                    "Attempt %d/%d with proxy=%s (available=%d, failed=%d, fetch_enabled=%s)",
                    attempt, MAX_RETRIES,
                    new_addr or "direct",
                    len(available_proxies) if PROXY_FETCH_ENABLED and self._pool.count > 0 else 0,
                    len(failed_proxies),
                    PROXY_FETCH_ENABLED,
                )

                try:
                    has_content = False
                    async for chunk in self._do_request(
                        candidate, messages, model, stream, **kw
                    ):
                        has_content = True
                        yield chunk
                    
                    if not has_content:
                        if new_addr:
                            failed_proxies.add(new_addr)
                        logger.debug(
                            "opencode retry %d/%d (proxy=%s): empty response",
                            attempt, MAX_RETRIES, new_addr or "direct",
                        )
                        if attempt < MAX_RETRIES:
                            retry_delay = min(2 ** (attempt - 1), 10)
                            await asyncio.sleep(retry_delay)
                        continue
                        
                    logger.debug(
                        "Request succeeded on attempt %d/%d with proxy=%s",
                        attempt, MAX_RETRIES, new_addr or "direct",
                    )
                    return
                    
                except PlatformError:
                    logger.warning(
                        "opencode attempt %d/%d: platform error, not retrying",
                        attempt, MAX_RETRIES,
                    )
                    raise
                    
                except RuntimeError as e:
                    last_exc = e
                    if new_addr:
                        failed_proxies.add(new_addr)
                    logger.warning(
                        "opencode retry %d/%d (proxy=%s failed): %s",
                        attempt, MAX_RETRIES, new_addr or "direct", e,
                    )
                    if attempt < MAX_RETRIES:
                        retry_delay = min(2 ** (attempt - 1), 10)
                        await asyncio.sleep(retry_delay)
                    continue
                    
                except GeneratorExit:
                    logger.debug(
                        "opencode generator closed externally during attempt %d/%d",
                        attempt, MAX_RETRIES,
                    )
                    raise
                    
                except Exception as e:
                    last_exc = e
                    if new_addr:
                        failed_proxies.add(new_addr)
                    logger.warning(
                        "opencode retry %d/%d (proxy=%s, unexpected error): %s",
                        attempt, MAX_RETRIES, new_addr or "direct", e,
                    )
                    if attempt < MAX_RETRIES:
                        retry_delay = min(2 ** (attempt - 1), 10)
                        await asyncio.sleep(retry_delay)
                    continue
                    
            logger.error(
                "All %d attempts failed (tried %d unique proxies), last error: %s",
                MAX_RETRIES, len(failed_proxies), last_exc,
            )
            if last_exc:
                raise PlatformError(
                    f"All {MAX_RETRIES} attempts failed with proxy pool"
                ) from last_exc
            else:
                raise PlatformError(
                    f"All {MAX_RETRIES} attempts returned empty responses"
                )
                
        finally:
            self._active_requests -= 1
            if self._active_requests == 0 and self._pool_refresh_pending:
                asyncio.ensure_future(self._check_deferred_refresh())

    async def _do_request(
        self,
        candidate: Candidate,
        messages: List[Dict],
        model: str,
        stream: bool,
        **kw: Any,
    ) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
        """执行单次HTTP请求，通过候选节点的代理或直连发送。

        评分记录统一在finally块中处理。
        PROXY_FETCH_ENABLED=False 时仍记录直连的评分（保留学习数据）。
        """
        proxy_addr = candidate.meta.get("proxy_addr", "")
        selector_key = proxy_addr if proxy_addr else DIRECT

        headers = build_headers(proxy_addr)
        payload = build_payload(messages, model, stream=stream, **kw)
        url = "{}{}".format(BASE_URL, CHAT_PATH)

        request_kwargs: Dict[str, Any] = dict(
            headers=headers,
            json=payload,
            ssl=False,
            timeout=aiohttp.ClientTimeout(
                connect=10,
                total=600 if stream else 120,
            ),
        )
        if proxy_addr:
            request_kwargs["proxy"] = "http://{}".format(proxy_addr)

        t0 = time.time()
        _request_ok = False
        _content_received = False
        _should_record_failure = True
        
        try:
            async with self._session.post(
                url,
                **request_kwargs,
            ) as resp:
                logger.debug(
                    "Response status=%d from proxy=%s",
                    resp.status, proxy_addr or "direct",
                )
                
                if resp.status != 200:
                    body = await resp.text()
                    
                    if resp.status == 429:
                        logger.debug(
                            "Rate limited (429) on proxy %s: %s",
                            proxy_addr, body[:200],
                        )
                        raise RuntimeError(
                            "opencode rate limited (429): {}".format(body[:200])
                        )
                    
                    if 500 <= resp.status < 600:
                        logger.debug(
                            "Server error (HTTP %d) on proxy %s: %s",
                            resp.status, proxy_addr, body[:200],
                        )
                        raise RuntimeError(
                            "opencode server error (HTTP {}): {}".format(
                                resp.status, body[:200]
                            )
                        )
                    else:
                        logger.warning(
                            "Client error (HTTP %d) on proxy %s: %s",
                            resp.status, proxy_addr, body[:200],
                        )
                        raise PlatformError(
                            "opencode HTTP{}: {}".format(resp.status, body[:200])
                        )

                if not stream:
                    data = await resp.json()
                    choice = (data.get("choices") or [{}])[0]
                    msg = choice.get("message", {})
                    content = msg.get("content", "")
                    
                    if content:
                        _content_received = True
                        _request_ok = True
                        _should_record_failure = False
                        yield content
                    
                    tc = msg.get("tool_calls")
                    if tc:
                        _content_received = True
                        _request_ok = True
                        _should_record_failure = False
                        yield {"tool_calls": tc}
                    
                    usage = data.get("usage")
                    if usage:
                        yield {"usage": {
                            "prompt_tokens": usage.get("prompt_tokens", 0),
                            "completion_tokens": usage.get("completion_tokens", 0),
                        }}
                    
                    if not _content_received:
                        logger.debug(
                            "Empty response content from proxy %s, finish_reason=%s",
                            proxy_addr, choice.get("finish_reason", "unknown"),
                        )
                        _request_ok = False
                        _should_record_failure = True
                        
                else:
                    _tc_accumulator: Dict[int, Dict[str, Any]] = {}
                    finish_reason = None
                    
                    try:
                        async for line in resp.content:
                            text = line.decode("utf-8", errors="replace").strip()
                            if not text or not text.startswith("data:"):
                                continue
                            data_str = text[5:].strip()
                            if data_str == "[DONE]":
                                break
                            parsed = parse_sse_line(data_str)
                            if parsed is None:
                                continue
                                
                            if isinstance(parsed, dict):
                                if "tool_calls" in parsed:
                                    _content_received = True
                                    for tc_delta in parsed["tool_calls"]:
                                        idx = tc_delta.get("index", 0)
                                        if idx not in _tc_accumulator:
                                            _tc_accumulator[idx] = {
                                                "id": "",
                                                "type": "function",
                                                "function": {"name": "", "arguments": ""},
                                            }
                                        acc = _tc_accumulator[idx]
                                        if tc_delta.get("id"):
                                            acc["id"] = tc_delta["id"]
                                        if tc_delta.get("type"):
                                            acc["type"] = tc_delta["type"]
                                        fn = tc_delta.get("function") or {}
                                        if fn.get("name"):
                                            acc["function"]["name"] += fn["name"]
                                        if fn.get("arguments"):
                                            acc["function"]["arguments"] += fn["arguments"]
                                elif isinstance(parsed, str):
                                    if parsed:
                                        _content_received = True
                                        yield parsed
                                else:
                                    choices = parsed.get("choices", [])
                                    if choices:
                                        delta = choices[0].get("delta", {})
                                        content = delta.get("content")
                                        if content:
                                            _content_received = True
                                            yield content
                                        finish_reason = choices[0].get("finish_reason")
                            elif isinstance(parsed, str):
                                if parsed:
                                    _content_received = True
                                    yield parsed
                        
                        if _content_received:
                            _request_ok = True
                            _should_record_failure = False
                        else:
                            logger.debug(
                                "Empty stream response from proxy %s, finish_reason=%s",
                                proxy_addr, finish_reason,
                            )
                            _request_ok = False
                            _should_record_failure = True
                        
                    except GeneratorExit:
                        if _content_received:
                            _request_ok = True
                            _should_record_failure = False
                        raise
                    except Exception as e:
                        logger.warning(
                            "Stream processing error with proxy %s: %s",
                            proxy_addr, e,
                        )
                        _request_ok = False
                        _should_record_failure = True
                        raise RuntimeError(
                            f"Stream processing error with proxy {proxy_addr}: {e}"
                        ) from e
                    
                    if _tc_accumulator:
                        tool_calls = [
                            v for _, v in sorted(_tc_accumulator.items())
                        ]
                        yield {"tool_calls": tool_calls}

        except aiohttp.ClientError as e:
            logger.warning(
                "Network error with proxy %s: %s (%s)",
                proxy_addr, type(e).__name__, e,
            )
            _request_ok = False
            _should_record_failure = True
            raise RuntimeError(
                f"Network error with proxy {proxy_addr}: {e}"
            ) from e
            
        except asyncio.TimeoutError as e:
            logger.warning(
                "Timeout with proxy %s after %.1fs: %s",
                proxy_addr, time.time() - t0, e,
            )
            _request_ok = False
            _should_record_failure = True
            raise RuntimeError(
                f"Timeout with proxy {proxy_addr}: {e}"
            ) from e
            
        except PlatformError:
            _request_ok = False
            _should_record_failure = True
            raise
            
        except RuntimeError:
            _request_ok = False
            _should_record_failure = True
            raise
            
        except Exception as e:
            logger.warning(
                "Unexpected error with proxy %s: %s (%s)",
                proxy_addr, type(e).__name__, e,
            )
            _request_ok = False
            _should_record_failure = True
            raise RuntimeError(
                f"Unexpected error with proxy {proxy_addr}: {e}"
            ) from e
            
        finally:
            # 始终记录评分（即使 PROXY_FETCH_ENABLED=False）
            # 这样切换到 True 时学习数据仍然存在
            if _request_ok and _content_received:
                latency_ms = (time.time() - t0) * 1000.0
                self._selector.record_success(selector_key, latency_ms)
                logger.debug(
                    "Request succeeded with proxy %s in %.0fms",
                    selector_key, latency_ms,
                )
            elif _should_record_failure:
                self._selector.record_failure(selector_key)
                logger.debug(
                    "Request failed with proxy %s, recorded as failure",
                    selector_key,
                )

    # ------------------------------------------------------------------
    # 远程模型获取
    # ------------------------------------------------------------------

    async def fetch_remote_models(self) -> List[str]:
        """从远程API获取可用模型列表（直连方式）。"""
        if not self._session:
            return []

        url = "{}{}".format(BASE_URL, MODELS_PATH)

        try:
            async with self._session.get(
                url,
                ssl=False,
                timeout=aiohttp.ClientTimeout(connect=10, total=30),
            ) as resp:
                if resp.status != 200:
                    logger.warning(
                        "opencode fetch models failed, HTTP%s", resp.status,
                    )
                    return []
                data = await resp.json()
                model_data = data.get("data", [])
                if isinstance(model_data, list):
                    all_models = [
                        m.get("id", "")
                        for m in model_data
                        if isinstance(m, dict) and m.get("id")
                    ]
                    if FILTER_PAID_MODELS:
                        return [m for m in all_models if m.endswith("-free")]
                    return all_models
                return []
        except Exception as e:
            logger.warning("opencode fetch models exception: %s", e)
            return []

    # ------------------------------------------------------------------
    # 模型列表管理
    # ------------------------------------------------------------------

    def get_available_models(self) -> List[str]:
        """返回当前可用模型列表。"""
        return list(self._models)

    def update_models(self, models: List[str]) -> None:
        """替换当前模型列表。"""
        self._models = list(models)

    # ------------------------------------------------------------------
    # 生命周期管理
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """取消后台任务，清理资源。"""
        if self._refresh_task is not None and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                logger.debug("opencode proxy refresh task cancelled")
