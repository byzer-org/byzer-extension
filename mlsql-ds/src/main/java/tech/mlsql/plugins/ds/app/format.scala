package tech.mlsql.plugins.ds.app

import scala.collection.mutable.ArrayBuffer

case class Xtreme1Point(x:Double,y:Double)
case class Xtreme1ResultObject(attrs: Map[String, String], color: String, frontId: String, objType: String, modelRun: String, classType: String,
                               coordinate: List[Xtreme1Point],modelClass: String)

case class Xtreme1Result(objects: List[Xtreme1ResultObject])

case class Xtreme1ContentData(image: String, result: Xtreme1Result)

case class Xtreme1Content(name: String, data: Xtreme1ContentData)

case class Xtreme1Format(version: String, datasetName: String, exportTime: String, contents: List[Xtreme1Content])


case class CocoInfoMsg(description: String,
                       url: String, version: String, year: Int, contributor: String, date_created: String)

case class CocoLicense(url: String, id: Int, name: String)

case class CocoImage(license: Int, file_name: String, coco_url: String, height: Int, width: Int, date_captured: String, flickr_url: String, id: Long)

case class CocoCategory(supercategory: String, id: Long, name: String, keypoints: List[String], skeleton: List[List[Int]])

case class CocoAnnotation(segmentation: List[List[Double]], area: Double,
                          iscrowd: Int, keypoints: List[Long],
                          image_id: Long, bbox: List[Double], category_id: Int, id: Long, caption: String)

case class CocoFormat(info: CocoInfoMsg,
                      licenses: List[CocoLicense],
                      images: List[CocoImage],
                      categories: List[CocoCategory],
                      annotations: List[CocoAnnotation]
                     )

object Formats {
  def toCocoFormat(x: Xtreme1Format) = {
    val info = CocoInfoMsg(x.datasetName, url = "", version = "1.0", year = 2017, contributor = "", date_created = x.exportTime)
    val license = List()
    val images = ArrayBuffer[CocoImage]()
    val annotations = ArrayBuffer[CocoAnnotation]()

    val models = x.contents.flatMap(item => item.data.result.objects.map(_.classType)).sorted
    val categories = models.zipWithIndex.map { case (name, index) => CocoCategory(name, index, name, List(), List()) }

    val categoryMap = categories.map(item => (item.name, item)).toMap

    x.contents.zipWithIndex.map { case (item, imgId) =>
      val fileName = item.data.image.split("\\?")(0).split("/").last
      images += CocoImage(0, fileName, coco_url = item.data.image, 0, 0, "", "", imgId)

      item.data.result.objects.map { obj =>
        val catagoryId = categoryMap.get(obj.classType).get.id
        annotations += CocoAnnotation(List(obj.coordinate.flatMap(temp => List(temp.x, temp.y))), 0, 0, List(), imgId,
          List(), category_id = catagoryId.toInt, imgId, "")
      }
    }
    CocoFormat(info, license, images.toList, categories, annotations.toList)
  }
}

