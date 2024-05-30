case "auc" =>         
  pyLang.raw.code(
    """
      |import sklearn.metrics as metrics
      |fpr, tpr, threshold = metrics.roc_curve(df['label'], df['probability'])
      |roc_auc = metrics.auc(fpr, tpr)
      |plt.figure()
      |_, ax = plt.subplots()
      |""".stripMargin).end
  val auc = pyLang.let("ax").invokeFunc("plot").params
    .add("fpr", None) 
    .add("tpr", None)
    .add("'b'", None)
    .addKV("label", "'AUC = %0.2f' % roc_auc", None)
  pyLang.raw.code(
    """
      |plt.legend(loc = 'lower right')
      |ax.plot([0, 1], [0, 1],'r--')
      |plt.xlim([0, 1])
      |plt.ylim([0, 1])
      |plt.ylabel('True Positive Rate')
      |plt.xlabel('False Positive Rate')  
      |""".stripMargin).end
  auc