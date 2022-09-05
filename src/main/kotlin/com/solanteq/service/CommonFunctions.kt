package com.solanteq.service

import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

class CommonFunctions {

    // формирует стандартную часть сообщения для лога:
    //    Class Object <ClassCode>, Object.refField=refFieldValue или
    //    Class Object <ClassCode>
    public fun createObjectIdForLogMsg(
        classCode: String,
        classRefField: String = "",
        objFieldsList: List<Fields>?,
        nestedLevel: Int
    ): String {
        return if (classRefField == "") {
            "Class Object <$classCode>. "
        } else {
            "Class Object <$classCode>, " +
                    "Object.${classRefField}=<${findValuesForKeyFieldOut(objFieldsList!!, classRefField)}>. " +
                    if (nestedLevel > -1) {
                        "NestedLevel=<$nestedLevel>. "
                    } else ""
        }
    }

    // формирует стандартную часть сообщения для лога:
    //    Class Object <ClassCode>, Object.refField=refFieldValue или
    //    Class Object <ClassCode>
    public fun createObjectIdForLogMsg(
        classCode: String,
        classRefField: String,
        keyFieldInValue: String
    ): String {
        return "Class Object <$classCode>, Object.${classRefField}=<${keyFieldInValue}>. "
    }

    // поиск значений полей из keyFieldOut в списке fields закачиваемого объекта (поиск в файле)
    // например, для keyFieldOut="code,numeric_code" вернет "RUR,810"
    // если значение одного из полей указанных в keyFieldOut будет null, то вылетаем с exception
    fun findValuesForKeyFieldOut(
        objFieldsList: List<Fields>,
        keyFieldOut: String
    ): String {
        var fieldValues = ""
        val arrRefField = keyFieldOut.split(",")
        // поля таблицы в keyFieldOut могут быть перечислены через запятую
        for (i in arrRefField.indices) {
            if (i > 0) {
                fieldValues += ","
            }
            fieldValues += objFieldsList.find { it.fieldName == arrRefField[i] }!!.fieldValue!!
        }

        return fieldValues
    }

    // формирования списка полей, которые не нужно выгружать, для каждого класса
    public fun createListFieldsNotExport(jsonConfigFile: RootCfg) {
        // формирую список полей для исключения в каждом классе конфигурации
        for (item in jsonConfigFile.objects) {
            for (field in CommonConstants().FIELDS_NOT_EXPORT) {
                item.fieldsNotExport.add(FieldsNotExport(field))
            }
        }
    }

    public fun checkObjectClass(allCheckObject: DataDBMain, jsonConfigFile: RootCfg, classCode:String) {
        val logger = LoggerFactory.getLogger(classCode)

        for (oneCheckObject in allCheckObject.element) {
            if (jsonConfigFile.objects.find { it.code == oneCheckObject.code } == null) {
                logger.error(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneCheckObject.code,
                        "",
                        null,
                        -1
                    ) + "There is no description of class in configuration file."
                )
                exitProcess(-1)
            }
        }
    }

    public fun checkObjectClass(allCheckObject: TaskFileFieldsMain, jsonConfigFile: RootCfg, classCode:String) {
        val logger = LoggerFactory.getLogger(classCode)

        for (oneCheckObject in allCheckObject.element) {
            if (jsonConfigFile.objects.find { it.code == oneCheckObject.code } == null) {
                logger.error(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneCheckObject.code,
                        "",
                        null,
                        -1
                    ) + "There is no description of class in configuration file."
                )
                exitProcess(-1)
            }
        }
    }
}