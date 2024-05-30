# ROC曲线可视化

在Byzer中,我们可以方便地使用YAML配置来绘制ROC曲线。ROC曲线(Receiver Operating Characteristic Curve)是一种常用于评估二分类模型性能的工具。通过绘制不同阈值下的真正率(True Positive Rate, TPR)和假正率(False Positive Rate, FPR),我们可以直观地评估模型的分类效果。

## 示例

下面是一个使用Byzer绘制ROC曲线的例子:

```sql
select p.label as label,m.probability as probability
from model_predict_result m
left join original_data p on p.id = m.id
as roc_data;

!visualize roc_data '''
fig:
  auc:
    title: "ROC Curve"
    x: fpr
    y: tpr
''';
```

在这个例子中:
- 我们首先准备好一张包含真实标签(`label`)和模型预测概率(`probability`)的表`roc_data`。 
- 然后使用`!visualize`命令,并在三引号中传入YAML配置。
- YAML配置中的`fig.auc`表示我们要绘制ROC曲线。
- `title`指定了图表的标题。
- `x`和`y`分别指定了图表的横纵坐标对应的列。

## 参数说明

ROC曲线的可视化参数如下:

| 参数 | 说明 |
| ---- | ---- |
| fig.auc.title | 图表的标题 | 
| fig.auc.x | 横坐标对应的列,通常是`fpr` |
| fig.auc.y | 纵坐标对应的列,通常是`tpr` |

除了上述ROC曲线特有的参数外,还可以使用通用的图表参数来控制图表的展示效果,如`control.backend`选择绘图引擎等。

通过组合使用这些参数,我们就可以非常方便地在Byzer中绘制ROC曲线,评估二分类模型的性能了。