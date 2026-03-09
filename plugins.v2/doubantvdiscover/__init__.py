import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple

from app import schemas
from app.chain.douban import DoubanChain
from app.chain.media import MediaChain
from app.core.event import Event, eventmanager
from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import DiscoverSourceEventData, MediaRecognizeConvertEventData
from app.schemas.types import ChainEventType, MediaType

DOUBAN_SORT = "R"
DOUBAN_AREAS = ["华语", "韩国"]
MAX_FETCH_COUNT = 400
MIN_RUNTIME = 25
DISCOVER_MEDIA_PREFIX = "doubanselect"
MIN_RETURN_COUNT = 100


class DoubanTVDiscover(_PluginBase):
    plugin_name = "新剧放送"
    plugin_desc = "探索中直接显示豆瓣电视剧，固定首播时间排序，地区为华语和韩国，仅保留时长大于25分钟。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/douban.png"
    plugin_version = "1.0.6"
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

        data["mediaid_prefix"] = DISCOVER_MEDIA_PREFIX
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

    @staticmethod
    def __apply_douban_detail(media: Dict[str, Any], detail: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(detail, dict):
            return media

        durations = detail.get("durations") or []
        if durations and not media.get("runtime"):
            match = re.search(r"\d+", str(durations[0]))
            if match:
                media["runtime"] = int(match.group())

        if not media.get("release_date"):
            if detail.get("release_date"):
                media["release_date"] = detail.get("release_date")
            elif detail.get("pubdate") and isinstance(detail.get("pubdate"), list):
                match = re.search(r"\d{4}-\d{2}-\d{2}", str(detail.get("pubdate")[0] or ""))
                if match:
                    media["release_date"] = match.group()

        if not media.get("year"):
            year_match = re.search(r"(19|20)\d{2}", str(media.get("release_date") or ""))
            if year_match:
                media["year"] = year_match.group()

        if not media.get("overview"):
            media["overview"] = detail.get("intro") or media.get("overview")
        return media

    async def __ensure_runtime(self, media: Dict[str, Any]) -> Dict[str, Any]:
        if self.__runtime_minutes(media) > 0:
            return media

        douban_id = str(media.get("media_id") or media.get("douban_id") or "").strip()
        if not douban_id:
            return media

        try:
            detail = await MediaChain().async_douban_info(
                doubanid=douban_id,
                mtype=MediaType.TV,
                raise_exception=False,
            )
        except Exception as err:
            logger.warning(f"补充豆瓣详情失败：{douban_id}，错误：{err}")
            return media

        return self.__apply_douban_detail(media=media, detail=detail)

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
        count = max(MIN_RETURN_COUNT, min(int(count), 100))
        fetch_count = min(max(page * count, MIN_RETURN_COUNT), MAX_FETCH_COUNT)

        tasks = [self.__fetch_area_medias(area=area, fetch_count=fetch_count) for area in DOUBAN_AREAS]
        fetched = await asyncio.gather(*tasks, return_exceptions=True)

        merged: Dict[str, Dict[str, Any]] = {}
        for area, result in zip(DOUBAN_AREAS, fetched):
            if isinstance(result, Exception):
                logger.error(f"获取豆瓣剧集探索数据失败，地区：{area}，错误：{result}")
                continue

            enriched_result = await asyncio.gather(
                *(self.__ensure_runtime(media) for media in result),
                return_exceptions=True,
            )
            for media in enriched_result:
                if isinstance(media, Exception):
                    logger.warning(f"补充豆瓣时长信息失败，错误：{media}")
                    continue
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

    @eventmanager.register(ChainEventType.DiscoverSource)
    def discover_source(self, event: Event):
        if not self._enabled:
            return

        event_data: DiscoverSourceEventData = event.event_data
        source = schemas.DiscoverMediaSource(
            name="豆瓣剧集精选",
            mediaid_prefix=DISCOVER_MEDIA_PREFIX,
            api_path=f"plugin/DoubanTVDiscover/douban_tv_discover?apikey={settings.API_TOKEN}",
            filter_params={
                "sort": "R",
                "area_group": "cn_kr",
                "runtime_filter": "gt25",
            },
            filter_ui=[],
        )
        if not event_data.extra_sources:
            event_data.extra_sources = [source]
        else:
            event_data.extra_sources.append(source)

    @eventmanager.register(ChainEventType.MediaRecognizeConvert)
    async def async_media_recognize_convert(self, event: Event):
        if not self._enabled:
            return

        event_data: MediaRecognizeConvertEventData = event.event_data
        if not event_data or not event_data.mediaid:
            return

        prefix = f"{DISCOVER_MEDIA_PREFIX}:"
        if not event_data.mediaid.startswith(prefix):
            return

        douban_id = event_data.mediaid[len(prefix):]
        if not douban_id:
            return

        if event_data.convert_type == "douban":
            event_data.media_dict["id"] = douban_id
            return

        if event_data.convert_type != "themoviedb":
            return

        tmdbinfo = await MediaChain().async_get_tmdbinfo_by_doubanid(
            doubanid=douban_id,
            mtype=MediaType.TV,
        )
        if tmdbinfo and tmdbinfo.get("id"):
            event_data.media_dict["id"] = tmdbinfo.get("id")

    def stop_service(self):
        pass
