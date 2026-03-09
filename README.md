# DoubanTVDiscover (MoviePilot v2)

独立版 `豆瓣剧集精选` 插件。

当前版本使用固定筛选参数形式向探索页提供数据源，兼容性更好。

## 目录结构

```text
DoubanTVDiscover-standalone/
├── package.v2.json
└── plugins.v2/
    └── doubantvdiscover/
        └── __init__.py
```

## 安装方式

### 方式一：作为独立插件仓库安装

1. 将本目录内容推送到你自己的 GitHub 仓库根目录。
2. 在 MoviePilot v2 中添加插件仓库地址（`package.v2.json` 的 Raw URL）。
3. 在插件市场搜索并安装 `豆瓣剧集精选`。

### 方式二：手动安装

1. 将 `plugins.v2/doubantvdiscover` 目录复制到你的插件仓库 `plugins.v2/` 下。
2. 将 `package.v2.json` 中的 `DoubanTVDiscover` 条目合并到你的仓库 `package.v2.json`。
3. 重启 MoviePilot 或刷新插件市场。

## 固定筛选条件

- 类型：电视剧
- 排序：首播时间
- 地区：华语、韩国
- 时长：大于 25 分钟

## 说明

- 插件会分别请求豆瓣剧集探索中的 `华语` 和 `韩国` 数据，再合并去重。
- 仅保留 MoviePilot 识别出的 `runtime > 25` 的条目，未识别到时长的剧集也会被过滤掉。
- 合并后会再次按首播日期倒序排列，保证结果更接近“首播时间”排序预期。
