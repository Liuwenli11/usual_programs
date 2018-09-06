package flink.udf.flow

import javassist.ClassPool
import org.apache.log4j.Logger

object UdfRegister {
  private lazy val logger = Logger.getLogger(this.getClass)

  def getUdfObject(className:String, methodName:String): Any ={
    val classPool = ClassPool.getDefault()
    val ctClass = classPool.get(className)

    ctClass.setSuperclass(classPool.get("org.apache.flink.table.functions.ScalarFunction"))

    val ctMethod = ctClass.getMethods.filter(m=>{
      m.getName == methodName
    }).head

    ctMethod.setName("eval")

    //ctClass.addMethod(ctMethod)

    ctClass.toClass.newInstance()
  }
}
