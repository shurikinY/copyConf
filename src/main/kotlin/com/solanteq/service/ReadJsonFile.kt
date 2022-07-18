package com.solanteq.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.File

data class RootCfg(
    val cfgList: List<CfgList>,
    val objects: List<ObjectCfg>
)

data class CfgList(
    val version: String,
    val dbType: String,
    val dateCreate: String,
    val auditDate: String?,
    val cfgName: String
)

data class ObjectCfg(
    val code: String,
    val name: String,
    //val aliasDb: String?,
    val tableName: String,
    val sequence: String,
    val tableNameAud: String,
    val keyFieldIn: String,
    val keyFieldOut: String,
    val auditDateField: String,
    var filterObjects: String = "",
    val linkObjects: List<LinkObjects>,
    val refObjects: List<RefObjects>,
    val fieldsNotExport: MutableList<FieldsNotExport>,
    val refFieldsJson: List<FieldsJson>,
    val refTables: List<RefTables>,
    val scale : List<RefObjects>? = null
)

data class LinkObjects(
    val refField: String,
    val codeRef: String,
    val mandatory: String?,
    val keyType: String
)

data class RefObjects(
    val record: String = "",
    val refField: String,
    val refFieldValue: String = "",
    val codeRef: String,
    val mandatory: String = "",
    val keyType: String,
    val refTypeArray: String = "refObjects"
)

data class FieldsNotExport(
    val name: String
)

data class FieldsJson(
    val name: String,
    val refObjects: List<RefObjects>?
)

data class RefTables(
    val table: String,
    val refField: String,
    val codeRef: String,
    val refFieldTo: String,
    val keyType: String
)

data class TaskFileFieldsMain(
    val cfgList: List<CfgList>,
    val element: List<TaskFileFields>
)

data class TaskFileFields(
    val code: String,
    val loadMode: String,
    val keyFieldIn: List<Fields>,
    val keyFieldOut: List<Fields>
)


// Разбор файла конфигурации
class ReadJsonFile {

    // считывание конфигурации в массив объектов
    /*fun <T> readJsonFile(fileName : String): List<T> {
        val mapper = jacksonObjectMapper()
        return mapper.readValue<List<T>>(File(fileName))
    }*/
    //inline fun <reified T> readTask(fileName : String) = jacksonObjectMapper().readValue<T>(File(fileName))

    // считывание конфигурации в список объектов
    fun readConfig(): RootCfg {
        return jacksonObjectMapper().readValue<RootCfg>(File(CONFIG_FILE))
    }

    // считывание файла фильтра в список объектов
    fun readConfigFilter(): FilterCfg {
        return jacksonObjectMapper().readValue<FilterCfg>(File(FILTER_FILE))
    }


    // считывание конфигурации в список объектов
    fun readTask(): TaskFileFieldsMain {
        return jacksonObjectMapper().readValue<TaskFileFieldsMain>(File(TASK_FILE))
    }

    // считывание строки в формате jsonStr
    fun readJsonStrAsTree(jsonStr: String): JsonNode {
        return jacksonObjectMapper().readValue(jsonStr)
    }

    // считывание файла объектов для загрузки в БД
    fun readObject(): DataDBMain {
        return jacksonObjectMapper().readValue<DataDBMain>(File(OBJECT_FILE))
    }

    // считывание файла объектов для загрузки в БД
    /*fun readDataSources(): DataSources {
        return jacksonObjectMapper().readValue<DataSources>(File(DATASOURCES_FILE))
    }*/

}
