# Byzer YAML Visualization

byzer-yaml-visualization 是一款 Byzer 可视化插件。通过该插件，用户可以通过 YAML 配置文件描述图表。

## 安装部署

可使用如下命令安装（需要有网络）：

```
!plugin app add - "byzer-yaml-visualization-3.0";
```

卸载:
                                                   
```
!plugin app remove  "byzer-yaml-visualization-3.0";
```

> 卸载需要重启引擎

## 使用示例

```sql
load excel.`./example-data/excel/user-behavior.xlsx` 
where header="true" as user_behavior;

select cast(datatime as date) as day,
       sum(case when behavior_type = 'pv' then 1 else 0 end) as pv,
       count(distinct user_id) as uv
from user_behavior
group by cast(datatime as date)
order by day as day_pv_uv;

!visualize day_pv_uv '''
fig:
    bar:
       title: "日PV/UV柱状图" 
       x: day
       y: 
        - pv
        - uv            
       labels: 
           day: "日期"           
''';
```

## 描述文档

YAML中的顶级元数有三个：
1. runtime  配置运行时。 YAML 文件会被转化为 Python 代码执行，所以runtime 其实是配置 Python环境。 
2. control  控制图表的一些生成行为，比如是生成html还是image，数据是不是再需要一次排序等等
3. fig      描绘生成什么样的图表，该图表的配置是什么

### runtime
runtime 下只有一层子元数，常见配置如下。
1. env 指定需要使用的 Python环境。 
2. cache 图表结果是不是要缓存，如果你在其他cell要引用这个图标结果，需要设置为true。默认设置为false 即可。
3. output 将图表转化为一个表引用，方便后续 SQL 使用。默认可以不用配置。
4. runIn  在哪个类型节点执行。 driver/executor 。推荐 driver。

### control

1. ignoreSort  默认为true. 系统会对 X 轴字段进行默认进行排序
2. format  默认为 html。 如果需要生成图片，可以设置为 `image`

### fig

1. fig.xxx  其中 xxx 为图标类型。支持 line,bar
2. fig.xxx.title 图表标题
3. fig.xxx.x X 轴。 支持字符串或者数组配置
4. fig.xxx.y Y 轴。 支持字符串或者数组配置
5. fig.xxx.labels 改动图标中的一些名称。 默认为字典

一个较为完整的配置如下：

```
runtime: 
   env: source /opt/miniconda3/bin/activate ray-1.12.0
   cache: false
   output: jack
control:
   ignoreSort: false
   format: image   
fig:
    bar:
       title: "日PV/UV柱状图" 
       x: day
       y: 
        - pv
        - uv            
       labels: 
           day: "日期"
```

## 图表类型示例

参考文章：



## 如何获取生成的图片

```sql
load excel.`./example-data/excel/user-behavior.xlsx` 
where header="true" as user_behavior;

select cast(datatime as date) as day,
       sum(case when behavior_type = 'pv' then 1 else 0 end) as pv,
       count(distinct user_id) as uv
from user_behavior
group by cast(datatime as date)
order by day as day_pv_uv;

!visualize day_pv_uv '''
runtime: 
   env: source /opt/miniconda3/bin/activate ray-1.12.0
   cache: false
   output: jack
control:
   ignoreSort: false
   format: image   
fig:
    bar:
       title: "日PV/UV柱状图" 
       x: day
       y: 
        - pv
        - uv            
       labels: 
           day: "日期"           
''';

select unbase64(content) as content, "wow.png" as fileName from jack as imageTable;

save overwrite imageTable as image.`/tmp/images` 
where imageColumn="content" 
and fileName="fileName";

!fs -ls /tmp/images;
```

图片默认使用 base64 编码，所以需要进行一次解码。然后使用 image 数据源把他写到对象存储里去。如果用户需要上传改图标，可以使用如下代码：

```sql
save overwrite command as Rest.`YOUR_UPLOAD_URL` 
where `config.method`="post"
and `header.content-type`="multipart/form-data"345
and `form.file-path`="/tmp/images/wow.png"
and `form.file-name`="wow.png";
```

