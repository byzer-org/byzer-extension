# ROC Visualization with Byzer YAML Visualization

## Introduction

This guide explains how to use the Byzer YAML Visualization extension to create Receiver Operating Characteristic (ROC) curve visualizations. ROC curves are commonly used to evaluate the performance of classification models by depicting the trade-off between true positive rate and false positive rate.

## Prerequisites

Before creating ROC visualizations, ensure that you have installed the byzer-yaml-visualization extension and have a classification dataset available in Byzer.

## YAML Configuration

Here's how to configure the YAML for generating an ROC curve:

```yaml
runtime:
   env: 
   cache: true
   output: roc_output
control:
   format: image
fig:
    auc:
       title: "ROC Curve" 
       labels: 
           x: "False Positive Rate"
           y: "True Positive Rate"
```

## SQL to Generate ROC Visualization

After setting up your data, you can invoke the visualization by using the following Byzer script:

```sql
load parquet.`path/to/your/data` as model_output;

select cast(probability as double) as probability,
       cast(label as int) as label
from model_output;

!visualize model_output '''
runtime:
   env: 
   cache: true
   output: roc_output
control:
   format: image   
fig:
    auc:
       title: "ROC Curve"
       labels: 
           x: "False Positive Rate"
           y: "True Positive Rate"           
''';

select unbase64(content) as content, "roc_curve.png" as fileName from roc_output as imageTable;

save overwrite imageTable as image.`/tmp/images`
where imageColumn="content" 
and fileName="fileName";
```

This script will process the data and produce an ROC curve visualization that is saved as an image. The `!visualize` command triggers the visualization based on the defined YAML configuration.

## Handling the Output

The output from the visualization is encoded in base64 format. To use or display the image:

```sql
!fs -ls /tmp/images;

-- To upload the image if needed:
save overwrite command as Rest.`YOUR_UPLOAD_URL` 
where `config.method`="post"
and `header.content-type`="multipart/form-data"
and `form.file-path`="/tmp/images/roc_curve.png"
and `form.file-name`="roc_curve.png";
```

This guide should help you understand and apply ROC curve visualization using Byzer YAML Visualization in your data analysis workflows.