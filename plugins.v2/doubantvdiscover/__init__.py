import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple

from app import schemas
from app.chain.douban import DoubanChain
from app.core.event import Event, eventmanager
from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import DiscoverSourceEventData
from app.schemas.types import ChainEventType, MediaType

DOUBAN_SORT = "R"
DOUBAN_AREAS = ["华语", "韩国"]
MAX_FETCH_COUNT = 200
MIN_RUNTIME = 25


class DoubanTVDiscover(_PluginBase):
    plugin_name = "豆瓣剧集精选"
    plugin_desc = "探索中直接显示豆瓣电视剧，固定首播时间排序，地区为华语和韩国，仅保留时长大于25分钟。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/douban.png"
    plugin_version = "1.0.1"
    plugin_author = "anxian"
    author_url = "https://github.com/jxxghp/MoviePilot-Plugins"
    plugin_config_prefix = "doubantvdiscover_"
    plugin_order = 99
    auth_level = 1

    _enabled = True

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = bool(config.get("enabled"))

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/douban_tv_discover",
                "endpoint": self.douban_tv_discover,
                "methods": ["GET"],
                "summary": "豆瓣剧集探索数据源",
                "description": "固定返回豆瓣电视剧，首播时间排序，地区为华语和韩国，仅保留时长大于25分钟",
            }
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ], {"enabled": True}

    def get_page(self) -> List[dict]:
        pass

    @staticmethod
    def __date_sort_key(media: Dict[str, Any]) -> Tuple[int, int, int, str]:
        date_text = str(
            media.get("release_date")
            or media.get("first_air_date")
            or media.get("year")
            or ""
        )
        parts = re.findall(r"\d+", date_text)
        year = int(parts[0]) if len(parts) > 0 else 0
        month = int(parts[1]) if len(parts) > 1 else 0
        day = int(parts[2]) if len(parts) > 2 else 0
        return year, month, day, str(media.get("title") or "")

    @staticmethod
    def __merge_category(current: Optional[str], area: str) -> str:
        values = [item.strip() for item in str(current or "").split("/") if item.strip()]
        if area not in values:
            values.append(area)
        return " / ".join(values)

    @staticmethod
    def __normalize_media(media: Any, area: str) -> Optional[Dict[str, Any]]:
        if not media:
            return None

        data = media.to_dict() if hasattr(media, "to_dict") else dict(media)
        douban_id = str(data.get("douban_id") or "").strip()
        if not douban_id:
            return None

        data["mediaid_prefix"] = "douban"
        data["media_id"] = douban_id
        data["category"] = area
        return data

    @staticmethod
    def __runtime_minutes(media: Dict[str, Any]) -> int:
        runtime = media.get("runtime")
        if isinstance(runtime, (int, float)):
            return int(runtime)

        match = re.search(r"\d+", str(runtime or ""))
        if match:
            return int(match.group())

        episode_run_time = media.get("episode_run_time") or []
        if isinstance(episode_run_time, list) and episode_run_time:
            first_value = episode_run_time[0]
            if isinstance(first_value, (int, float)):
                return int(first_value)
            match = re.search(r"\d+", str(first_value or ""))
            if match:
                return int(match.group())
        return 0

    async def __fetch_area_medias(self, area: str, fetch_count: int) -> List[Dict[str, Any]]:
        medias = await DoubanChain().async_douban_discover(
            mtype=MediaType.TV,
            sort=DOUBAN_SORT,
            tags=area,
            page=1,
            count=fetch_count,
        )
        results: List[Dict[str, Any]] = []
        for media in medias or []:
            info = self.__normalize_media(media=media, area=area)
            if info:
                results.append(info)
        return results

    async def douban_tv_discover(
        self,
        sort: str = "R",
        area_group: str = "cn_kr",
        runtime_filter: str = "gt25",
        page: int = 1,
        count: int = 30,
    ) -> List[schemas.MediaInfo]:
        _ = sort, area_group, runtime_filter
        page = max(1, int(page))
        count = max(1, min(int(count), 100))
        fetch_count = min(max(page * count, count), MAX_FETCH_COUNT)

        tasks = [self.__fetch_area_medias(area=area, fetch_count=fetch_count) for area in DOUBAN_AREAS]
        fetched = await asyncio.gather(*tasks, return_exceptions=True)

        merged: Dict[str, Dict[str, Any]] = {}
        for area, result in zip(DOUBAN_AREAS, fetched):
            if isinstance(result, Exception):
                logger.error(f"获取豆瓣剧集探索数据失败，地区：{area}，错误：{result}")
                continue

            for media in result:
                if self.__runtime_minutes(media) <= MIN_RUNTIME:
                    continue
                media_id = str(media.get("media_id") or "")
                if not media_id:
                    continue
                if media_id in merged:
                    merged[media_id]["category"] = self.__merge_category(
                        merged[media_id].get("category"), area
                    )
                    continue
                merged[media_id] = media

        medias = sorted(merged.values(), key=self.__date_sort_key, reverse=True)
        start = (page - 1) * count
        end = start + count
        return [schemas.MediaInfo(**media) for media in medias[start:end]]

    @staticmethod
    def douban_filter_ui() -> List[dict]:
        def chip(value: str, text: str) -> dict:
            return {
                "component": "VChip",
                "props": {"filter": True, "tile": True, "value": value},
                "text": text,
            }

        return [
            {
                "component": "div",
                "props": {"class": "flex justify-start items-center"},
                "content": [
                    {
                        "component": "div",
                        "props": {"class": "mr-5"},
                        "content": [{"component": "VLabel", "text": "排序"}],
                    },
                    {
                        "component": "VChipGroup",
                        "props": {"model": "sort"},
                        "content": [chip("R", "首播时间")],
                    },
                ],
            },
            {
                "component": "div",
                "props": {"class": "flex justify-start items-center"},
                "content": [
                    {
                        "component": "div",
                        "props": {"class": "mr-5"},
                        "content": [{"component": "VLabel", "text": "地区"}],
                    },
                    {
                        "component": "VChipGroup",
                        "props": {"model": "area_group"},
                        "content": [chip("cn_kr", "华语 + 韩国")],
                    },
                ],
            },
            {
                "component": "div",
                "props": {"class": "flex justify-start items-center"},
                "content": [
                    {
                        "component": "div",
                        "props": {"class": "mr-5"},
                        "content": [{"component": "VLabel", "text": "时长"}],
                    },
                    {
                        "component": "VChipGroup",
                        "props": {"model": "runtime_filter"},
                        "content": [chip("gt25", "大于 25 分钟")],
                    }
                ],
            }
        ]

    @eventmanager.register(ChainEventType.DiscoverSource)
    def discover_source(self, event: Event):
        if not self._enabled:
            return

        event_data: DiscoverSourceEventData = event.event_data
        source = schemas.DiscoverMediaSource(
            name="豆瓣剧集精选",
            mediaid_prefix="douban",
            api_path=f"plugin/DoubanTVDiscover/douban_tv_discover?apikey={settings.API_TOKEN}",
            filter_params={
                "sort": "R",
                "area_group": "cn_kr",
                "runtime_filter": "gt25",
            },
            filter_ui=self.douban_filter_ui(),
        )
        if not event_data.extra_sources:
            event_data.extra_sources = [source]
        else:
            event_data.extra_sources.append(source)

    def stop_service(self):
        pass
