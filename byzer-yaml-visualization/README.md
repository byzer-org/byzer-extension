# Byzer YAML Visualization
                          
## Introduction

byzer-yaml-visualization is a visualization extension for Byzer. 
It can generate charts based on the data returned by Byzer script.

## Online 

### Installation

Make sure public network is available.

```
!plugin app add - "byzer-yaml-visualization-3.3";
```

### Uninstall
                                                   
```
!plugin app remove  "byzer-yaml-visualization-3.3";
```

> Note: Restarting Byzer after installation or uninstallation is required.

## Usage

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

## YAML Format

There are three top-level elements in YAML:
1. runtime:  Configure the runtime. The YAML Format will be converted to Python code for execution, so the runtime actually configures the Python environment.
2. control:  Control some generation behaviors of the chart, such as whether to generate html or image, whether the data needs to be sorted again, etc.
3. fig:      Describe what kind of chart to generate, and what is the configuration of the chart


### runtime

runtime is the configuration of the Python environment. The normal configuration is as follows.

1. env:  Specify the Python environment to use.
2. cache:  Whether to cache the chart result. If you want to reference the chart result in other cells, you need to set it to true. The default setting is false.
3. output:  Convert the chart to a table reference for subsequent SQL use. 
4. runIn:  Which type of node to execute. driver/executor. It is recommended to use driver.


### control

1. ignoreSort  Default is true. The system will sort the X-axis field by default
2. format  Default is html. If you need to generate an image, you can set it to `image`

### fig

1. fig.xxx  Where xxx is the type of chart. line,bar are supported
2. fig.xxx.title Chart title
3. fig.xxx.x X axis. Support string or array configuration
4. fig.xxx.y Y axis. Support string or array configuration
5. fig.xxx.labels Change some names in the chart. The default is a dictionary

### Example

One complete configuration is as follows:


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

## More examples

https://zhuanlan.zhihu.com/p/538701145



## How to get the chart result

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

The chart is encoded in base64 format. You need to decode it first. Then use the image data source to write it to the object storage. 
If the user needs to upload this chart, you can use the following code:


```sql
save overwrite command as Rest.`YOUR_UPLOAD_URL` 
where `config.method`="post"
and `header.content-type`="multipart/form-data"345
and `form.file-path`="/tmp/images/wow.png"
and `form.file-name`="wow.png";
```

