package ksb.csle.component.pipe.query.parser

import ksb.csle.common.base.pipe.query.parser.BaseParser

class MyDLParser extends BaseParser[String, String] {
  override def parse(item: String): String = item
}
