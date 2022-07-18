package com.solanteq.service

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import kotlin.system.exitProcess
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

data class DataDBMain(
    val cfgList: List<CfgList>,
    val element: List<DataDB>
)

data class DataDB(
    val code: String,
    val loadMode: String,
    val row: Row
)

data class Row(
    var fields: List<Fields>,
    val refObject: List<RefObject>,
    var linkObjects: List<DataDB>,
    var scaleObjects: List<DataDB>
)

data class Fields(
    val fieldName: String,
    var fieldValue: String?
)

data class RefObject(
    val code: String,                   // класс ссылочного объекта
    val nestedLevel: Int,               // уровень ссылочного объекта
    val typeRef: String,                // тип ссылочного объекта refObjects, refFieldsJson, refTables
    val fieldRef: String,               // наименование ссылочного поля
    val row: RefRow,
    var refObject: List<RefObject>
)

data class RefRow(
    var fields: List<Fields>
)

data class FilterCfg(
    val include: List<FilterFields>,
    val exclude: List<FilterFields>
)

data class FilterFields(
    val code: String,
    val saveMode: String,
    val filterObjects: String
)

// Чтение данных из БД согласно конфигурации. Создание json
class ReaderDB {

    // считывание конфигурации
    val readJsonFile = ReadJsonFile()
    val jsonConfigFile = readJsonFile.readConfig()

    var scalable = Scalable(jsonConfigFile)

    private val logger = LoggerFactory.getLogger(javaClass)

    // список ссылочных объектов для одного основного
    private var tblRefObject = mutableListOf<RefObject>()

    // после выполнения readAllObject здесь будет список всех выкачиваемых объектов (в формате DataDB)
    private var tblMain = mutableListOf<DataDB>()

    // после выполнения readAllObject здесь будет список всех выкачиваемых объектов (в формате DataDB) + информация о версии конфигурации
    public var tblMainMain = DataDBMain(listOf<CfgList>(), listOf<DataDB>())

    // после выполнения readAllObject здесь будет список объектов (TaskFileFields). нужно для формирования файла заданий
    private var taskFileFields = mutableListOf<TaskFileFields>()

    // после выполнения readAllObject здесь будет список объектов (TaskFileFields) + информация о версии конфигурации
    public var taskFileFieldsMain = TaskFileFieldsMain(listOf<CfgList>(), listOf<TaskFileFields>())

    // список объектов, описанных в linkObject
    private var tblLinkObject = mutableListOf<DataDB>()

    // список объектов, описанных в scaleObject
    private var tblScaleObject = mutableListOf<DataDB>()

    // коннект к БД
    private lateinit var conn: Connection

    // создание файла заданий
    fun readAllObject() {

        // считывание конфигурации
        //val readJsonFile = ReadJsonFile()
        //val jsonConfigFile = readJsonFile.readConfig()

        // считывание файла фильтра
        var jsonConfigFilterFile: FilterCfg? = null
        if (FILTER_FILE != "") {
            jsonConfigFilterFile = readJsonFile.readConfigFilter()
        }

        logger.info("Configuration info: " + jsonConfigFile.cfgList[0])

        // формирования списка полей, которые не нужно выгружать, для каждого класса
        CommonFunctions().createListFieldsNotExport(jsonConfigFile)

        // установка соединения с БД
        //logger.info("DataBase connection parameters: dbconn=$CONN_STRING dbuser=$CONN_LOGIN dbpass=$CONN_PASS ")
        try {
            conn = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
        } catch (e: Exception) {
            logger.error("Error connection to DataBase: " + e.message)
            exitProcess(-1)
        }

        // - цикл по конфигурации(по всем таблицам)
        //     В случае если у объекта в конфигурации не заполнено поле keyFieldOut = "", т.е. объект не может быть идентифицирован в БД приемнике,
        //     он исключается из выборки объектов в режиме формирования файла задания. При этом нужно добавить в выборку объектов его родительский объект.
        //     В дальнейшем объекты класса, у которого  keyFieldOut = "", могут быть выгружены только через структуру linkObjects родительского объекта.
        //     Используется, например, для объектов типа "Ставка тарифа(tariffValue)", не имеющих уникального внешнего кода идентификации и хранящихся с условием периода с..по...
        // - если в файле фильтре в массиве include указаны классы, то выкачиваем объекты только указанных классов. Также на объекты этих классов накладываем дополнительные фильтры
        // - если в файле фильтре в массиве exclude указаны класс и не указан фильтр(поле filterObjects), то весь класс исключается из выборки
        // - если в файле фильтре в массиве exclude указаны класс и указан фильтр, то на объекты этих класса накладываем дополнительные фильтры
        if (jsonConfigFilterFile != null) {
            for (excludeClass in jsonConfigFilterFile.exclude) {
                val oneConfigClass =
                    jsonConfigFile.objects.find { it.code == excludeClass.code && it.keyFieldOut != "" }
                if (oneConfigClass != null && excludeClass.filterObjects != "") {
                    if (oneConfigClass.filterObjects != "") {
                        oneConfigClass.filterObjects += " and ${excludeClass.filterObjects}"
                    } else {
                        oneConfigClass.filterObjects += excludeClass.filterObjects
                    }
                }
            }
            for (includeClass in jsonConfigFilterFile.include) {
                val oneConfigClass =
                    jsonConfigFile.objects.find { it.code == includeClass.code && it.keyFieldOut != "" }
                if (oneConfigClass != null) {
                    if (oneConfigClass.filterObjects != "" && includeClass.filterObjects != "") {
                        oneConfigClass.filterObjects += " and ${includeClass.filterObjects}"
                    } else if (includeClass.filterObjects != "") {
                        oneConfigClass.filterObjects += includeClass.filterObjects
                    }
                }
            }
        }
        // если задан массив include, то выгружаются только объекты классов указанные в нем
        if (jsonConfigFilterFile != null && jsonConfigFilterFile.include.isNotEmpty()) {
            for (includeClass in jsonConfigFilterFile.include) {
                val oneConfigClass =
                    jsonConfigFile.objects.find { it.code == includeClass.code && it.keyFieldOut != "" }
                if (oneConfigClass != null) {
                    //readOneObject(null, oneConfigClass, jsonConfigFile, false, "")
                    readOneObject(null, oneConfigClass, jsonConfigFile, "", 0)
                }
            }
            // если задан массив exclude с пустым полем filterObjects, то объекты классов указанные в нем исключаются из выгрузки
        } else if (jsonConfigFilterFile != null && jsonConfigFilterFile.exclude.isNotEmpty()) {
            for (oneConfigClass in jsonConfigFile.objects.filter { it.keyFieldOut != "" }) {
                if (jsonConfigFilterFile.exclude.find { it.code == oneConfigClass.code && it.filterObjects == "" } == null) {
                    //readOneObject(null, oneConfigClass, jsonConfigFile, false, "")
                    readOneObject(null, oneConfigClass, jsonConfigFile, "", 0)
                }
            }
            // если массив include и массив exclude пустые или файл фильтров не указан
        } else {
            for (oneConfigClass in jsonConfigFile.objects.filter { it.keyFieldOut != "" }) {
                //readOneObject(null, oneConfigClass, jsonConfigFile, false, "")
                readOneObject(null, oneConfigClass, jsonConfigFile, "", 0)
            }
        }
        conn.close()

        val localDateTime = LocalDateTime.now()
        val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val currentDate = localDateTime.format(datetimeFormatter)
        taskFileFieldsMain = TaskFileFieldsMain(
            listOf(
                CfgList(
                    jsonConfigFile.cfgList[0].version,
                    jsonConfigFile.cfgList[0].dbType,
                    currentDate,
                    AUDIT_DATE,
                    jsonConfigFile.cfgList[0].cfgName
                )
            ), taskFileFields
        )
    }

    // формирование объектов на основе файла задания
    fun readTaskObjects() {

        // считывание конфигурации
        //val readJsonFile = ReadJsonFile()
        //val jsonConfigFile = readJsonFile.readConfig()
        //val jsonConfigFile = configJson.readJsonFile<RootCfg>(CONFIG_FILE)

        logger.info("Configuration info: " + jsonConfigFile.cfgList[0])

        // формирования списка полей, которые не нужно выгружать, для каждого класса
        CommonFunctions().createListFieldsNotExport(jsonConfigFile)

        // считывание файла заданий
        val jsonTaskFile = readJsonFile.readTask()
        //val jsonTaskFile = configJson.readJsonFile<TaskFileFields>(TASK_FILE)

        // установка даты аудита из файла задания
        //AUDIT_DATE = jsonTaskFile.cfgList[0].auditDate!!

        // установка соединения с БД
        //logger.info("DataBase connection parameters: dbconn=$CONN_STRING dbuser=$CONN_LOGIN dbpass=$CONN_PASS ")
        try {
            conn = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
        } catch (e: Exception) {
            logger.error("Error connection to DataBase: " + e.message)
            exitProcess(-1)
        }

        // цикл по объектам из файла заданий
        for (oneTaskObject in jsonTaskFile.element) {
            val oneConfigClass =
                jsonConfigFile.objects[jsonConfigFile.objects.indexOfFirst { it.code == oneTaskObject.code }]
            //readOneObject(oneTaskObject, oneConfigClass, jsonConfigFile, false, "")
            readOneObject(oneTaskObject, oneConfigClass, jsonConfigFile, "", 0)

            if (false) {
                // цикл по linkObjects с keyType=In объекта из файла заданий и добавление linkObjects в файл выгрузки как главных объектов
                for (oneLinkObjectIn in oneConfigClass.linkObjects.filter { it.keyType.lowercase() == "in" }) {
                    if (oneTaskObject.loadMode.lowercase() == "safe.linkobjects") {
                        readLinkObjectIn(oneTaskObject, oneConfigClass, jsonConfigFile, oneLinkObjectIn)
                    }
                }
            }

        }

        conn.close()
        tblMainMain = DataDBMain(jsonConfigFile.cfgList, tblMain)
        val localDateTime = LocalDateTime.now()
        val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val currentDate = localDateTime.format(datetimeFormatter)
        tblMainMain = DataDBMain(
            listOf(
                CfgList(
                    jsonConfigFile.cfgList[0].version,
                    jsonConfigFile.cfgList[0].dbType,
                    currentDate,
                    jsonTaskFile.cfgList[0].auditDate,
                    jsonConfigFile.cfgList[0].cfgName
                )
            ), tblMain
        )
    }

    //формирование одного объекта
    private fun readOneObject(
        oneTaskObject: TaskFileFields?,
        oneConfigClass: ObjectCfg,
        jsonConfigFile: RootCfg,
        //isSaveLinkObject: Boolean, // режим выгрузки массива linkObjects. Может принимать true только в режиме save, при выгрузке объектов linkObjects
        scaleQuery: String, // Заполняется в случаи выгрузки шкал, в остальных случаях пусто. может быть заполнено только в режиме save
        linkObjectLevel: Int = 0 // признак выгрузки массива linkObjects. Может принимать true только в режиме save, при выгрузке объектов linkObjects.
    ) {

        // чтение таблицы
        val sqlQuery = if (scaleQuery != "") {
            scaleQuery
        } else {
            createMainSqlQuery(
                oneConfigClass,
                oneTaskObject,
                jsonConfigFile,
                //isSaveLinkObject,
                linkObjectLevel
            )
        }
        logger.trace(
            CommonFunctions().createObjectIdForLogMsg(oneConfigClass.code, "", null, -1) +
                    "Query to the main object: $sqlQuery"
        )
        val queryStatement = conn.prepareStatement(sqlQuery)
        val queryResult = queryStatement.executeQuery()

        //if (!queryResult.isBeforeFirst && REGIM == CommonConstants().REGIM_CREATE_OBJFILE && !isSaveLinkObject) {
        if (!queryResult.isBeforeFirst && REGIM == CommonConstants().REGIM_CREATE_OBJFILE && linkObjectLevel == 0) {
            logger.error(
                CommonFunctions().createObjectIdForLogMsg(
                    oneConfigClass.code,
                    oneConfigClass.keyFieldIn,
                    oneTaskObject!!.keyFieldIn,
                    -1
                ) + "<The main object was not found in the source database>"
            )
            exitProcess(-1)
        }

        // запись данных БД в массив объектов для json
        while (queryResult.next()) {
            val tblFields = mutableListOf<Fields>()
            for (i in 1..queryResult.metaData.columnCount) {
                // название поля таблицы
                val fieldName = queryResult.metaData.getColumnName(i)
                // исключение из результирующего массива не нужных полей
                if (oneConfigClass.fieldsNotExport.find { it.name == fieldName } != null) {
                    continue
                }
                // значение поля таблицы
                val fieldValue = queryResult.getString(i)
                tblFields.add(Fields(fieldName, fieldValue))
            }

            // рекурсивный поиск и запись ссылочных объектов в список tblRefObject
            if (REGIM == CommonConstants().REGIM_CREATE_OBJFILE) {

                //readRefObject(oneConfigClass, jsonConfigFile, tblFields, isSaveLinkObject, 1)
                readRefObject(oneConfigClass, jsonConfigFile, tblFields, linkObjectLevel, 1)

                //if (!isSaveLinkObject && scaleQuery == "") {
                if (linkObjectLevel == 0 && scaleQuery == "") {
                    tblMain.add(
                        DataDB(
                            oneConfigClass.code,
                            oneTaskObject!!.loadMode,
                            Row(tblFields, tblRefObject, listOf<DataDB>(), listOf<DataDB>())
                        )
                    )
                    tblRefObject = mutableListOf<RefObject>()
                }

                if (linkObjectLevel > 0) {
                    val dataDBObject = DataDB(
                        oneConfigClass.code,
                        oneTaskObject!!.loadMode,
                        Row(tblFields, tblRefObject, listOf<DataDB>(), listOf<DataDB>())
                    )

                    if (tblLinkObject.isEmpty()) {
                        tblLinkObject.add(dataDBObject)
                    } else {
                        tblLinkObject[tblLinkObject.lastIndex].row.linkObjects += dataDBObject
                    }
                    tblRefObject = mutableListOf<RefObject>()
                }
                readLinkObjectInGroup(oneConfigClass, jsonConfigFile, tblFields, linkObjectLevel)
                //if (isSaveLinkObject) {
                //if (linkObjectLevel > 0) {
                if (linkObjectLevel == 1) {
                    /*tblLinkObject.add(
                        DataDB(
                            oneConfigClass.code,
                            oneTaskObject!!.loadMode,
                            Row(tblFields, tblRefObject, listOf<DataDB>(), listOf<DataDB>())
                        )
                    )*/
                    tblMain[tblMain.lastIndex].row.linkObjects += tblLinkObject
                    tblLinkObject = mutableListOf<DataDB>()
                    tblRefObject = mutableListOf<RefObject>()
                }

                // выгрузка тарифных шкал. шкалы выгружаются только для родительского класса numberTariffValue и tariffValue
                readScaleObject(oneConfigClass, jsonConfigFile, tblFields, "Read", listOf<DataDB>())
                if (scaleQuery != "") {
                    tblScaleObject.add(
                        DataDB(
                            oneConfigClass.code,
                            oneTaskObject!!.loadMode,
                            Row(tblFields, tblRefObject, listOf<DataDB>(), listOf<DataDB>())
                        )
                    )
                    val linkObjectLastIndex = tblMain[tblMain.lastIndex].row.linkObjects.lastIndex
                    tblMain[tblMain.lastIndex].row.linkObjects[linkObjectLastIndex].row.scaleObjects =
                        tblMain[tblMain.lastIndex].row.linkObjects[linkObjectLastIndex].row.scaleObjects.plus(
                            tblScaleObject
                        )

                    /*tblMain[tblMain.lastIndex].row.linkObjects[linkObjectLastIndex].row.scaleObjects =
                        tblMain[tblMain.lastIndex].row.linkObjects[linkObjectLastIndex].row.scaleObjects?.plus(
                            tblScaleObject
                        )*/
                    tblScaleObject = mutableListOf<DataDB>()
                    tblRefObject = mutableListOf<RefObject>()
                }
            }

            if (REGIM == CommonConstants().REGIM_CREATE_TASKFILE) {

                val lstKeyFieldInValue =
                    getOneFieldFromRefField(tblFields, oneConfigClass, oneConfigClass.keyFieldIn)
                val lstKeyFieldOutValue =
                    getOneFieldFromRefField(tblFields, oneConfigClass, oneConfigClass.keyFieldOut)
                taskFileFields.add(
                    TaskFileFields(
                        oneConfigClass.code,
                        "Safe",
                        lstKeyFieldInValue,
                        lstKeyFieldOutValue
                    )
                )

            }
        }
        queryStatement.close()
    }

    // рекурсивное чтение ссылочных объектов (списки refObjects, refFieldsJson, refTables в конфиге)
    private fun readRefObject(
        jsonCfgOneObj: ObjectCfg,       // один класс из конфига
        jsonCfgAllObj: RootCfg,         // все классы из конфига
        tblFieldsOneObj: List<Fields>,  // список полей объекта класса jsonCfgOneObj в виде {название поля, значение поля}
        //isSaveLinkObject: Boolean,      // режим выгрузки массива linkObjects
        linkObjectLevel: Int,
        nestedLevel: Int,               // уровень вложенности ссылочного объекта
    ) {

        // формирование однородного списка референсов для разных типов списков(списки refObjects, refFieldsJson, refTables)
        val refObjects =
            //createListToFindRefObjects(jsonCfgOneObj, tblFieldsOneObj, jsonCfgAllObj, isSaveLinkObject, nestedLevel)
            createListToFindRefObjects(jsonCfgOneObj, tblFieldsOneObj, jsonCfgAllObj, nestedLevel, linkObjectLevel)

        // цикл по ссылкам класса
        for (itemRefObject in refObjects) {

            var tblRefObjectLocal: RefObject
            val codeRef = itemRefObject.codeRef
            // поиск класса по ссылке
            if (jsonCfgAllObj.objects.find { it.code == codeRef } != null) {
                // проверка на то, что ссылочное поле не null
                if (tblFieldsOneObj.find { it.fieldName == itemRefObject.refField }?.fieldValue != null ||
                    (itemRefObject.refTypeArray == "refTables" && itemRefObject.refFieldValue.isNotEmpty()) //typeRefField == "refTables"
                ) {
                    val jsonConfigObject =
                        jsonCfgAllObj.objects[jsonCfgAllObj.objects.indexOfFirst { it.code == codeRef }]

                    // формирование запроса для выкачки референсного объекта
                    val sqlFieldValue =
                        createRefSqlQuery(
                            jsonConfigObject,
                            tblFieldsOneObj,
                            itemRefObject,
                            nestedLevel
                        )
                    val queryFieldValue = conn.prepareStatement(sqlFieldValue)
                    val resultFieldValue = queryFieldValue.executeQuery()

                    // ошибка: нет объекта, на который есть ссылка в объекте верхнего уровня
                    if (!resultFieldValue.isBeforeFirst) {
                        var refFieldName = ""
                        if (itemRefObject.keyType.lowercase() == "in" || itemRefObject.keyType.lowercase() == "inparent" || itemRefObject.keyType.lowercase() == "inchild" ||
                            itemRefObject.keyType.lowercase() == "inparentscale" || itemRefObject.keyType.lowercase() == "inscale"
                        ) {
                            refFieldName = jsonConfigObject.keyFieldIn
                        } else if (itemRefObject.keyType.lowercase() == "out") {
                            refFieldName = jsonConfigObject.keyFieldOut
                        }
                        logger.error(
                            CommonFunctions().createObjectIdForLogMsg(
                                itemRefObject.codeRef, refFieldName, findFieldValue(tblFieldsOneObj, itemRefObject)
                            ) + "<The object doesn't exists>"
                        )
                        exitProcess(-1)
                    }

                    // запись данных БД в массив объектов для json
                    var tblRefRow: RefRow //= RefRow(listOf<Fields>())
                    //var tblRefRow = mutableListOf<RefRow>()
                    while (resultFieldValue.next()) {
                        val tblFields = mutableListOf<Fields>()
                        for (i in 1..resultFieldValue.metaData.columnCount) {

                            // название поля таблицы
                            val fieldName = resultFieldValue.metaData.getColumnName(i)

                            // исключение из результирующего массива не нужных полей
                            if (jsonConfigObject.fieldsNotExport.find { it.name == fieldName } != null) {
                                continue
                            }

                            // значение поля таблицы
                            val fieldValue = resultFieldValue.getString(i)

                            // для объекта  последнего уровня рекурсии будут считаны только два поля: идентификатор и код
                            if (nestedLevel == CommonConstants().NESTED_LEVEL_REFERENCE) {
                                if (jsonConfigObject.keyFieldOut.contains(fieldName, ignoreCase = true)) {
                                    //tblFields.add(Fields(jsonConfigObject.keyFieldOut, fieldValue))
                                    tblFields.add(Fields(fieldName, fieldValue))
                                } else if (jsonConfigObject.keyFieldIn.contains(fieldName, ignoreCase = true)) {
                                    //tblFields.add(Fields(jsonConfigObject.keyFieldIn, fieldValue))
                                    tblFields.add(Fields(fieldName, fieldValue))
                                }
                                continue
                            }
                            tblFields.add(Fields(fieldName, fieldValue))
                        }

                        // для референса 2 уровня среди его keyFieldOut не должно быть поля - референса (нет кода для такого ключа)
                        if (nestedLevel == 2) {
                            checkFldOutForLink(
                                jsonConfigObject,
                                tblFields,
                                jsonConfigObject.keyFieldOut/*, "keyFieldOut"*/
                            )
                        }

                        //tblRefRow.add(RefRow(tblFields))
                        tblRefRow = RefRow(tblFields)
                        tblRefObjectLocal = RefObject(
                            jsonConfigObject.code,
                            nestedLevel,
                            itemRefObject.refTypeArray,
                            itemRefObject.refField,
                            tblRefRow,
                            mutableListOf<RefObject>()
                        )

                        // добавляю новый уровень вложенности в референс первого уровня
                        if (nestedLevel > 1) {
                            tblRefObject[tblRefObject.lastIndex].refObject += mutableListOf(tblRefObjectLocal)
                        } else { // добавляю референс первого уровня
                            tblRefObject.add(tblRefObject.size, tblRefObjectLocal)
                        }

                        // ограничение рекурсии
                        if (nestedLevel < CommonConstants().NESTED_LEVEL_REFERENCE) {
                            /*readRefObject(
                                jsonConfigObject,
                                jsonCfgAllObj,
                                tblFields,
                                isSaveLinkObject,
                                nestedLevel + 1
                            )*/
                            readRefObject(
                                jsonConfigObject,
                                jsonCfgAllObj,
                                tblFields,
                                linkObjectLevel,
                                nestedLevel + 1
                            )
                        }

                    }
                    queryFieldValue.close()
                } else {
                    // поле указанное в референсе не найдено в таблице класса, в котором описан референс
                    if (tblFieldsOneObj.find { it.fieldName == itemRefObject.refField } == null) {
                        logger.error(
                            CommonFunctions().createObjectIdForLogMsg(
                                jsonCfgOneObj.code,
                                jsonCfgOneObj.keyFieldIn,
                                tblFieldsOneObj,
                                -1
                            ) + "<The name of the refField=${itemRefObject.refField} specified in the reference was not found in class table ${jsonCfgOneObj.tableName}>"

                        )
                        exitProcess(-1)
                    }
                    // значение поля, указанного в референсе = null
                    if (itemRefObject.mandatory.lowercase() == "t") {
                        logger.error(
                            CommonFunctions().createObjectIdForLogMsg(
                                jsonCfgOneObj.code,
                                jsonCfgOneObj.keyFieldIn,
                                tblFieldsOneObj,
                                -1
                            ) + "<The value of the refField=${itemRefObject.refField} (lvl $nestedLevel) specified in the reference for class table ${jsonCfgOneObj.tableName} is null>"
                        )
                        exitProcess(-1)
                    } else if (itemRefObject.mandatory.lowercase() != "t") {
                        logger.warn(
                            CommonFunctions().createObjectIdForLogMsg(
                                jsonCfgOneObj.code,
                                jsonCfgOneObj.keyFieldIn,
                                tblFieldsOneObj,
                                -1
                            ) + "<The value of the refField=${itemRefObject.refField} (lvl $nestedLevel) specified in the reference for class table ${jsonCfgOneObj.tableName} is null, but mandatory=F>"
                        )
                    }

                }
            } else {
                logger.error(
                    CommonFunctions().createObjectIdForLogMsg(
                        jsonCfgOneObj.code,
                        jsonCfgOneObj.keyFieldIn,
                        tblFieldsOneObj,
                        -1
                    ) + "<The reference class definition <$codeRef> not find>"
                )
                exitProcess(-1)
            }
        }
    }

    // формирование запроса для поиска главного объекта
    private fun createMainSqlQuery(
        oneConfigClass: ObjectCfg,
        oneTaskObject: TaskFileFields?,
        jsonConfigFile: RootCfg,
        //isSaveLinkObject: Boolean,
        linkObjectLevel: Int
    ): String {

        val tableName = oneConfigClass.tableName

        var auditDateObjCond = ""
        if (REGIM == CommonConstants().REGIM_CREATE_TASKFILE) {
            auditDateObjCond = " and ${oneConfigClass.auditDateField} > to_timestamp(' $AUDIT_DATE ','YYYY-MM-DD') "
        }

        var filterObjCond = ""
        if (oneConfigClass.filterObjects != "") {
            filterObjCond = " and ${oneConfigClass.filterObjects} "
        }

        var sqlQuery: String

        // дополнительное условие на id главного объекта (именно главного, не linkObject)
        var dopCondForMainObjId = ""

        // дополнительное условие для объекта linkObjects(например, tariffCondition) на ид основного объекта(например, tariff)
        var dopCondForLinkObjId = ""

        if (REGIM == CommonConstants().REGIM_CREATE_OBJFILE) {

            //if (isSaveLinkObject ) {
            if (linkObjectLevel > 0) {
                val mainObjClass = jsonConfigFile.objects.find { it.code == oneTaskObject!!.code }!!
                val mainObjId =
                    oneTaskObject!!.keyFieldIn.find { it.fieldName == mainObjClass.keyFieldIn }!!.fieldValue

                //for (item in mainObjClass.linkObjects.filter { (it.keyType.lowercase() == "ingroup") && it.codeRef == oneConfigClass.code }) {
                for (item in mainObjClass.linkObjects.filter { it.codeRef == oneConfigClass.code }) {
                    val linkFieldForMainTable = item.refField
                    dopCondForLinkObjId = " and $linkFieldForMainTable=$mainObjId "
                    break
                }
            } else {
                dopCondForMainObjId =
                    " and ${oneConfigClass.keyFieldIn} = ${oneTaskObject!!.keyFieldIn[0].fieldValue}"
            }
        }

        // При формировании файла задания для объекта с настроенным списком подчиненных объектов linkObjects с типом "keyType": "InGroup"
        //  необходимо проверять по auditDateField вместе с родительским все подчиненные объекты.
        //  Родительский объект попадает в файл задания если был изменен хотя бы один из подчиненных.
        //if (oneConfigClass.linkObjects.filter { it.keyType.lowercase() == "ingroup" }.isNotEmpty()
        if (oneConfigClass.linkObjects.isNotEmpty() && linkObjectLevel == 0) {
            sqlQuery =
                "\nselect * from  $tableName where audit_state = 'A' $filterObjCond and ${oneConfigClass.keyFieldIn} in (\n "
            sqlQuery += "select ${oneConfigClass.keyFieldIn} from $tableName where audit_state = 'A' $auditDateObjCond $filterObjCond $dopCondForMainObjId\n "

            //for (oneCfgLinkObj in oneConfigClass.linkObjects.filter { it.keyType.lowercase() == "ingroup" }) {
            for (oneCfgLinkObj in oneConfigClass.linkObjects) {

                val oneCfgLinkObjClass = jsonConfigFile.objects.find { it.code == oneCfgLinkObj.codeRef }!!

                var filterObjCondLink = ""
                if (oneCfgLinkObjClass.filterObjects != "") {
                    filterObjCondLink = " and ${oneCfgLinkObjClass.filterObjects} "
                }

                // формирую дополнительное условие на id главного объекта
                if (REGIM == CommonConstants().REGIM_CREATE_OBJFILE) {

                    val mainObjClass = jsonConfigFile.objects.find { it.code == oneTaskObject!!.code }!!
                    val mainObjId =
                        oneTaskObject!!.keyFieldIn.find { it.fieldName == mainObjClass.keyFieldIn }!!.fieldValue

                    //for (item in mainObjClass.linkObjects.filter { it.keyType.lowercase() == "ingroup" && it.codeRef == oneCfgLinkObjClass.code }) {
                    for (item in mainObjClass.linkObjects.filter { it.codeRef == oneCfgLinkObjClass.code }) {
                        val linkFieldForMainTable = item.refField
                        dopCondForLinkObjId = " and $linkFieldForMainTable=$mainObjId "
                        break
                    }
                }

                var auditDateObjCondLink = ""
                if (REGIM == CommonConstants().REGIM_CREATE_TASKFILE) {
                    auditDateObjCondLink =
                        " and ${oneCfgLinkObjClass.auditDateField} > to_timestamp(' $AUDIT_DATE ','YYYY-MM-DD') "
                }

                sqlQuery += "union \n select ${oneCfgLinkObj.refField} from ${oneCfgLinkObjClass.tableName} where audit_state = 'A' " +
                        "$auditDateObjCondLink $filterObjCondLink $dopCondForLinkObjId\n "
            }
            sqlQuery += ") " + "order by id"
        } else {
            sqlQuery = "select * " +
                    "from  $tableName " +
                    "where audit_state = 'A' " +
                    "$auditDateObjCond " +
                    "$filterObjCond " +
                    "$dopCondForLinkObjId " +
                    "$dopCondForMainObjId " +
                    "order by id"
        }

        return sqlQuery
    }

    // формирование запроса для поиска референсного объекта
    private fun createRefSqlQuery(
        jsonConfigObject: ObjectCfg,
        tblFieldsOneObj: List<Fields>,
        itemRefObject: RefObjects,
        nestedLevel: Int
    ): String {

        val sqlQuery: String
        var refField = ""
        var conditionSql = ""

        if (itemRefObject.keyType.lowercase() == "in" || itemRefObject.keyType.lowercase() == "inparent" || itemRefObject.keyType.lowercase() == "inchild" ||
            itemRefObject.keyType.lowercase() == "inparentscale" || itemRefObject.keyType.lowercase() == "inscale"
        ) {
            refField = jsonConfigObject.keyFieldIn
        } else if (itemRefObject.keyType.lowercase() == "out") {
            refField = jsonConfigObject.keyFieldOut
        }

        //val refFieldValue: String?
        val refFieldValue = findFieldValue(tblFieldsOneObj, itemRefObject)

        var fieldValueForLog = ""
        val arrRefField = refField.split(",")
        // поля таблицы в keyFieldOut могут быть перечислены через запятую
        for (i in arrRefField.indices) {
            if (i > 0) {
                conditionSql += " or "
                fieldValueForLog += ","
            }
            conditionSql += "(" + arrRefField[i] + "='" + refFieldValue + "')"
            fieldValueForLog += refFieldValue
        }
        conditionSql = "($conditionSql)"

        if (jsonConfigObject.filterObjects != "") {
            conditionSql += " and ${jsonConfigObject.filterObjects} "
        }

        sqlQuery = "select * " +
                "from ${jsonConfigObject.tableName} where audit_state = 'A' and $conditionSql " +
                "order by id"
        logger.trace(
            CommonFunctions().createObjectIdForLogMsg(jsonConfigObject.code, refField, fieldValueForLog) +
                    "Query to the reference object (lvl ${nestedLevel}): $sqlQuery"
        )

        return sqlQuery
    }

    // поиск полей перечисленных через символ "," и запись их в список Fields
    private fun getOneFieldFromRefField(
        tblFields: List<Fields>,
        oneConfigClass: ObjectCfg,
        lstField: String
    ): List<Fields> {
        val returnTblFields = mutableListOf<Fields>()
        val arrKeyFieldOut = lstField.split(",")
        for (item in arrKeyFieldOut) {
            val itemValue = tblFields[tblFields.indexOfFirst { it.fieldName == item }].fieldValue
            if (!itemValue.isNullOrEmpty()) {
                returnTblFields.add(Fields(item, itemValue))
            } else {
                logger.error(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneConfigClass.code,
                        oneConfigClass.keyFieldIn,
                        tblFields,
                        -1
                    ) + "No value was found for the field <$item> specified in the class keyFieldOut."
                )
                exitProcess(-1)
            }
        }
        return returnTblFields
    }

    // формирование однородного списка референсов для разных типов ссылок(списки refObjects, refFieldsJson, refTables, scale)
    private fun createListToFindRefObjects(
        jsonCfgOneObj: ObjectCfg,
        tblFieldsOneObj: List<Fields>,
        jsonCfgAllObj: RootCfg,
        //isSaveLinkObject: Boolean,
        nestedLevel: Int,
        linkObjectLevel: Int
    ): List<RefObjects> {

        val refObjects = mutableListOf<RefObjects>()
        val listItemRefObject = mutableListOf<RefObjects>()

        // обработка списка референсов refObjects и scale
        for (itemRefObject in jsonCfgOneObj.refObjects) {
            listItemRefObject.add(itemRefObject)
        }
        jsonCfgOneObj.scale?.let { scaleRefObjects ->
            for (itemRefObject in scaleRefObjects.filter { it.keyType.lowercase() == "inparentscale" || it.keyType.lowercase() == "inscale" }) {
                listItemRefObject.add(itemRefObject)
            }
        }
        for (itemRefObject in listItemRefObject) {

            // при выгрузке объектов linkObjects не обрабатываем референсы на главный объект(тот, по которому выгружаем linkObjects), иначе будет кольцо
            //if (isSaveLinkObject && itemRefObject.codeRef == tblMain[tblMain.lastIndex].code) {
            if (linkObjectLevel > 0 &&
                (itemRefObject.codeRef == tblMain.last().code ||
                        (tblLinkObject.isNotEmpty() && tblLinkObject.last().code == itemRefObject.codeRef))) {
                continue
            }

            if (itemRefObject.keyType.lowercase() == "out" && itemRefObject.refField.contains(",")) {
                logger.error(
                    CommonFunctions().createObjectIdForLogMsg(
                        jsonCfgOneObj.code,
                        "",
                        null,
                        -1
                    ) + "The class has an out type refObject to the class <${itemRefObject.codeRef}> with a composite key <${itemRefObject.refField}>."
                )
                exitProcess(-1)
            }
            if (itemRefObject.keyType.lowercase() == "inparent" || itemRefObject.keyType.lowercase() == "inchild" ||
                itemRefObject.keyType.lowercase() == "inparentscale" || itemRefObject.keyType.lowercase() == "inscale"
            ) {
                refObjects.add(
                    RefObjects(
                        itemRefObject.record,
                        itemRefObject.refField,
                        itemRefObject.refFieldValue,
                        itemRefObject.codeRef,
                        itemRefObject.mandatory,
                        itemRefObject.keyType,
                        itemRefObject.keyType
                    )
                )
            } else {
                refObjects.add(itemRefObject)
            }
        }

        // обработка списка референсов refFieldsJson
        for (oneRefFieldsJson in jsonCfgOneObj.refFieldsJson) {
            if (!oneRefFieldsJson.refObjects.isNullOrEmpty()) {

                var filterObjCond = ""
                if (jsonCfgOneObj.filterObjects != "") {
                    filterObjCond = " and ${jsonCfgOneObj.filterObjects} "
                }

                // взять значение в виде строки Json из поля атрибутов
                val sqlFieldValue =
                    "select ${oneRefFieldsJson.name} " +
                            "from ${jsonCfgOneObj.tableName} " +
                            " where audit_state = 'A' and ${jsonCfgOneObj.keyFieldIn} = ${tblFieldsOneObj.find { it.fieldName == jsonCfgOneObj.keyFieldIn }!!.fieldValue} " +
                            filterObjCond +
                            " order by id"
                logger.trace(
                    CommonFunctions().createObjectIdForLogMsg(
                        jsonCfgOneObj.code,
                        jsonCfgOneObj.keyFieldIn,
                        tblFieldsOneObj,
                        -1
                    ) + "Query to find \"refFieldsJson\". " + sqlFieldValue
                )

                val queryFieldValue = conn.prepareStatement(sqlFieldValue)
                val queryResult = queryFieldValue.executeQuery()
                if (!queryResult.isBeforeFirst) {
                    logger.error(
                        CommonFunctions().createObjectIdForLogMsg(
                            jsonCfgOneObj.code,
                            jsonCfgOneObj.keyFieldIn,
                            tblFieldsOneObj,
                            -1
                        ) + "<The object doesn't exists>"
                    )
                    exitProcess(-1)
                }
                queryResult.next()
                for (oneRefFieldJson in oneRefFieldsJson.refObjects) {

                    val oneRefObjClass = jsonCfgAllObj.objects.find { it.code == oneRefFieldJson.codeRef }!!

                    val jsonStr = queryResult.getString(1)
                    if (jsonStr == null) {
                        logger.warn(
                            CommonFunctions().createObjectIdForLogMsg(
                                jsonCfgOneObj.code,
                                jsonCfgOneObj.keyFieldIn,
                                tblFieldsOneObj,
                                -1
                            ) + "<The value ${jsonCfgOneObj.tableName}.${oneRefFieldsJson.name} is null>"
                        )
                        break
                    }

                    // В строке Json хранящейся в поле таблицы ищу запись refFieldsJson.record.
                    try {
                        // зачитываю json-строку атрибутов в дерево
                        val attribute = ReadJsonFile().readJsonStrAsTree(jsonStr)
                        val attributeRecord = attribute[oneRefFieldJson.record]
                        val fieldNameInJsonStr = oneRefFieldJson.refField
                        if (attributeRecord != null && attributeRecord.size() > 0) {
                            // для каждого значения референса из строки атрибутов ищу референс среди референсов переданного объекта
                            for (itemFromAttribute in attributeRecord) {

                                // ид референса в бд источнике
                                var fieldValueInJsonStr: String
                                if (itemFromAttribute is ObjectNode) { // для вариантов вида {\"restrictions\":[{\"finInstitutionId\":\"100001\",\"restrictionActionType\":\"BLOCK\"},{\"finInstitutionId\":\"100002\",\"restrictionActionType\":\"BLOCK\"}]}
                                    fieldValueInJsonStr = itemFromAttribute.get(fieldNameInJsonStr).asText()
                                } else if (attributeRecord is ObjectNode) { // для вариантов вида {\"restrictions\":{\"finInstitutionId\":\"100001\",\"restrictionActionType\":\"BLOCK\"}}
                                    fieldValueInJsonStr = attributeRecord.get(fieldNameInJsonStr).asText()
                                } else { // для вариантов вида {"systemGroupList":[100000,100002]}; {"systemGroupList":[100000]}
                                    fieldValueInJsonStr = itemFromAttribute.asText()
                                }
                                refObjects.add(
                                    RefObjects(
                                        oneRefFieldJson.record,
                                        oneRefObjClass.keyFieldIn,
                                        fieldValueInJsonStr,//item.get(oneRefFieldJson.refField).asText(),
                                        oneRefFieldJson.codeRef,
                                        oneRefFieldJson.mandatory,
                                        oneRefFieldJson.keyType,
                                        "refFieldsJson"
                                    )
                                )
                                // это для случаев вида \"restrictions\":{\"finInstitutionId\":\"100001\",\"restrictionActionType\":\"BLOCK\"}},
                                // когда в itemFromAttribute попадают "finInstitutionId":"100001, затем "restrictionActionType":"BLOCK"
                                if (attributeRecord !is ArrayNode) {
                                    break
                                }
                            }
                        }
                    } catch (e: Exception) {
                        logger.error(
                            CommonFunctions().createObjectIdForLogMsg(
                                jsonCfgOneObj.code,
                                jsonCfgOneObj.keyFieldIn,
                                tblFieldsOneObj,
                                -1
                            ) + "<The problem with parsing the structure \"refFieldsJson\">. " + e.message
                        )
                        exitProcess(-1)
                    }
                }
                queryFieldValue.close()
            }
        }

        // обработка списка референсов refTables
        for (oneLinkRefObj in jsonCfgOneObj.refTables) {

            val indObj = jsonCfgAllObj.objects.indexOfFirst { it.code == oneLinkRefObj.codeRef }
            if (indObj < 0) {
                continue
            }

            // нужно найти название поля для соединения с таблицей класса искомого объекта(ов).
            // название поля есть в refTables связанного класса
            // в новом варианте конфигурации появилось поле refFieldTo в refTables
            val linkFieldName =
                if (oneLinkRefObj.refFieldTo != "") {
                    oneLinkRefObj.refFieldTo
                } else {
                    jsonCfgAllObj.objects[indObj].refTables.find { it.table == oneLinkRefObj.table }!!.refField
                }
            // название таблицы связанного класса
            val linkTableName = jsonCfgAllObj.objects[indObj].tableName
            // поле keyFieldIn связанного класса
            val linkKeyFieldIn = jsonCfgAllObj.objects[indObj].keyFieldIn

            var filterObjCond = ""
            if (jsonCfgAllObj.objects[indObj].filterObjects != "") {
                filterObjCond = " and ${jsonCfgAllObj.objects[indObj].filterObjects} "
            }

            val sqlFieldValue = "select distinct rel_tbl.$linkKeyFieldIn " +
                    "from ${oneLinkRefObj.table} link_tbl " +
                    "join $linkTableName rel_tbl on rel_tbl.$linkKeyFieldIn = link_tbl.$linkFieldName " +
                    "where rel_tbl.audit_state = 'A' and link_tbl.${oneLinkRefObj.refField} = ${tblFieldsOneObj.find { it.fieldName == jsonCfgOneObj.keyFieldIn }?.fieldValue}" +
                    filterObjCond
            logger.trace(
                CommonFunctions().createObjectIdForLogMsg(
                    jsonCfgOneObj.code,
                    jsonCfgOneObj.keyFieldIn,
                    tblFieldsOneObj,
                    -1
                ) + "Query to find \"refTables\". " + sqlFieldValue
            )

            val queryFieldValue = conn.prepareStatement(sqlFieldValue)
            val queryResult = queryFieldValue.executeQuery()
            if (!queryResult.isBeforeFirst) {
                continue
            }
            while (queryResult.next()) {

                val fieldValue = queryResult.getString(1)
                if (fieldValue == null) {
                    logger.error(
                        CommonFunctions().createObjectIdForLogMsg(
                            jsonCfgOneObj.code,
                            jsonCfgOneObj.keyFieldIn,
                            tblFieldsOneObj,
                            -1
                        ) + "<The value ${linkTableName}.${linkKeyFieldIn} is null OR There is no relation in table ${oneLinkRefObj.table}>"
                    )
                    exitProcess(-1)
                }

                refObjects.add(
                    RefObjects(
                        "",
                        oneLinkRefObj.refField,
                        queryResult.getString(1).toString(),
                        oneLinkRefObj.codeRef,
                        "",
                        oneLinkRefObj.keyType,
                        "refTables"
                    )
                )
            }
            queryFieldValue.close()
        }

        return refObjects
    }

    // поиск значения ссылочного поля
    private fun findFieldValue(
        tblFieldsOneObj: List<Fields>,
        itemRefObject: RefObjects
    ): String {
        val refFieldValue =
            if (itemRefObject.refTypeArray == "refTables" && itemRefObject.refFieldValue != "") {
                itemRefObject.refFieldValue
            } else if (itemRefObject.refTypeArray == "refFieldsJson" && tblFieldsOneObj.find { it.fieldName == itemRefObject.refField } != null && itemRefObject.refFieldValue != "") {
                itemRefObject.refFieldValue
            } else {
                (tblFieldsOneObj.find { it.fieldName == itemRefObject.refField })!!.fieldValue!!
            }
        return refFieldValue
    }

    // если в классе объекта в keyFieldOut указано поле, которое при этом является ссылкой типа refObjects,
    // то считаю, что такой объект нельзя проверить
    fun checkFldOutForLink(
        oneConfigClass: ObjectCfg,
        tblFields: List<Fields>?,
        stringFields: String
    ) {

        val arrKeyFieldName = stringFields.split(",")

        for (fieldOutName in arrKeyFieldName) {
            val oneRefObjFromClass =
                oneConfigClass.refObjects.find { it.refField == fieldOutName && it.keyType.lowercase() != "inparent" }

            if (oneRefObjFromClass != null) {
                logger.error(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneConfigClass.code,
                        oneConfigClass.keyFieldOut,
                        tblFields,
                        -1
                    ) + "The class of reference object lvl 2 has reference field in its keyFieldOut <$fieldOutName> and this reference field has not <InParent> keyType."
                )
                exitProcess(-1)
            }
        }
    }

    // выгрузка linkObject.keyType=InGroup. выгружаются как подчиненные объекты
    private fun readLinkObjectInGroup(
        oneConfigClass: ObjectCfg,     // один класс из конфига
        jsonConfigFile: RootCfg,       // все классы из конфига
        tblFieldsOneObj: List<Fields>,  // список полей объекта класса jsonCfgOneObj в виде {название поля, значение поля}
        linkObjectLevel: Int,
    ) {

        if (oneConfigClass.linkObjects.isNotEmpty()) {
            //for (oneCfgLinkObj in oneConfigClass.linkObjects.filter { it.keyType.lowercase() == "ingroup" || it.keyType.lowercase() == "in" }) {
            for (oneCfgLinkObj in oneConfigClass.linkObjects) {
                val oneCfgLinkObjClass = jsonConfigFile.objects.find { it.code == oneCfgLinkObj.codeRef }!!

                taskFileFields = mutableListOf<TaskFileFields>()

                val lstKeyFieldInValue =
                    getOneFieldFromRefField(tblFieldsOneObj, oneConfigClass, oneConfigClass.keyFieldIn)
                // у вложенного linkobject может быть пустое keyFieldOut, в этом случае его не обрабатываем
                var lstKeyFieldOutValue = listOf<Fields>()
                if (linkObjectLevel == 0 || oneConfigClass.keyFieldOut != "") {
                    lstKeyFieldOutValue =
                        getOneFieldFromRefField(tblFieldsOneObj, oneConfigClass, oneConfigClass.keyFieldOut)
                }
                val taskObject =
                    TaskFileFields(oneConfigClass.code, "Safe", lstKeyFieldInValue, lstKeyFieldOutValue)

                //readOneObject(taskObject, oneCfgLinkObjClass, jsonConfigFile, true, "", linkObjectLevel + 1)
                readOneObject(taskObject, oneCfgLinkObjClass, jsonConfigFile, "", linkObjectLevel + 1)
            }
        }

    }


    // выгрузка scaleObjects. выгружаются как подчиненные объекты
    public fun readScaleObject(
        oneConfigClass: ObjectCfg,     // один класс из конфига
        jsonConfigFile: RootCfg,       // все классы из конфига
        tblFieldsOneObj: List<Fields>, // список полей объекта класса jsonCfgOneObj в виде {название поля, значение поля}
        objectEvent: String,           // Read - формирование scaleObjects, Load - проверка при загрузке scaleObjects
        scaleObjects: List<DataDB>
    ): Boolean {

        val scalable = Scalable(jsonConfigFile)
        //if ((oneConfigClass.code.lowercase() == CommonConstants().NUMBER_TARIFF_VALUE_CLASS_NAME || oneConfigClass.code.lowercase() == CommonConstants().TARIFF_VALUE_CLASS_NAME) &&
        if (scalable.isClassHaveScaleComponent(oneConfigClass) &&
            !oneConfigClass.scale.isNullOrEmpty() &&
            //tblFieldsOneObj.find { it.fieldName == CommonConstants().SCALE_COMPONENT_ID_FIELD_NAME }!!.fieldValue != null
            tblFieldsOneObj.find { it.fieldName == scalable.scaleComponentFieldName }!!.fieldValue != null
        ) {

            // выгрузка объектов scaleComponent. Ссылка по референсу с keyType=InScaleComponent из класса numbertariffvalue или tariffvalue
            val trfValueScaleObjects = oneConfigClass.scale
            //oneConfigClass.scale.let { trfValueScaleObjects ->
            val trfValueScaleReference =
                trfValueScaleObjects.find { it.keyType == "InScaleComponent" }
            trfValueScaleReference?.let { trfValueScaleRef ->
                val scaleComponentClass =
                    jsonConfigFile.objects.find { it.code == trfValueScaleRef.codeRef }!!

                val scaleComponentId =
                    tblFieldsOneObj.find { it.fieldName == trfValueScaleRef.refField }!!.fieldValue

                var filterObjCond = ""
                if (scaleComponentClass.filterObjects != "") {
                    filterObjCond = " and ${scaleComponentClass.filterObjects} "
                }
                var sqlQuery = "select * from  ${scaleComponentClass.tableName} " +
                        " where audit_state = 'A' and ${scaleComponentClass.keyFieldIn} = $scaleComponentId " +
                        filterObjCond +
                        " order by id "

                val lstKeyFieldInValue =
                    getOneFieldFromRefField(tblFieldsOneObj, oneConfigClass, oneConfigClass.keyFieldIn)
                val lstKeyFieldOutValue = listOf<Fields>()
                val taskObject =
                    TaskFileFields(oneConfigClass.code, "Safe", lstKeyFieldInValue, lstKeyFieldOutValue)
                if (objectEvent == "Read") {
                    taskFileFields = mutableListOf<TaskFileFields>()
                    //readOneObject(taskObject, scaleComponentClass, jsonConfigFile, false, sqlQuery)
                    readOneObject(taskObject, scaleComponentClass, jsonConfigFile, sqlQuery, 0)
                } else if (objectEvent == "Load") {

                    // проверка scaleObjects
                    // для объектов класса scaleComponent не нужна проверка. там нет значимых полей
                    val isAddScaleObjects =
                        compareScaleObjects(
                            sqlQuery,
                            scaleComponentClass,
                            scaleObjects,
                            oneConfigClass,
                            tblFieldsOneObj
                        )
                    if (!isAddScaleObjects) {
                        return false
                    }
                }

                // выгрузка объектов scaleComponentValue. Ссылка по референсу с keyType=InScaleСomponentValue из класса scaleComponent
                scaleComponentClass.scale?.let { scaleComponentScaleObjects ->
                    val scaleComponentReference =
                        scaleComponentScaleObjects.find { it.keyType == "InScaleComponentValue" }
                    scaleComponentReference?.let { scaleComponentRef ->
                        val scaleComponentValueClass =
                            jsonConfigFile.objects.find { it.code == scaleComponentRef.codeRef }!!

                        filterObjCond = ""
                        if (scaleComponentValueClass.filterObjects != "") {
                            filterObjCond = " and ${scaleComponentValueClass.filterObjects} "
                        }
                        sqlQuery = " select * from  ${scaleComponentValueClass.tableName} " +
                                " where audit_state = 'A' and ${scaleComponentRef.refField} = $scaleComponentId " +
                                filterObjCond +
                                " order by id "

                        if (objectEvent == "Read") {
                            taskFileFields = mutableListOf<TaskFileFields>()
                            //readOneObject(taskObject, scaleComponentValueClass, jsonConfigFile, false, sqlQuery)
                            readOneObject(taskObject, scaleComponentValueClass, jsonConfigFile, sqlQuery, 0)
                        } else if (objectEvent == "Load") {

                            // проверка scaleObjects
                            val isAddScaleObjects =
                                compareScaleObjects(
                                    sqlQuery,
                                    scaleComponentValueClass,
                                    scaleObjects,
                                    oneConfigClass,
                                    tblFieldsOneObj
                                )
                            if (!isAddScaleObjects) {
                                return false
                            }
                        }
                        // выгрузка объектов scalableAmount. Ссылка по референсу с keyType=InScalableAmount из класса scaleComponentValue
                        scaleComponentValueClass.scale?.let { scaleComponentValueScaleObjects ->
                            val scaleAmountReference =
                                scaleComponentValueScaleObjects.find { it.keyType == "InScalableAmount" }
                            scaleAmountReference?.let { scaleAmountRef ->
                                val scaleAmountClass =
                                    jsonConfigFile.objects.find { it.code == scaleAmountRef.codeRef }!!

                                filterObjCond = ""
                                if (scaleAmountClass.filterObjects != "") {
                                    filterObjCond = " and ${scaleAmountClass.filterObjects} "
                                }
                                sqlQuery = " select distinct sc_amount.* " +
                                        " from  ${scaleComponentValueClass.tableName} sc_comp " +
                                        " join ${scaleAmountClass.tableName} sc_amount on sc_amount.${scaleAmountClass.keyFieldIn} = sc_comp.${scaleAmountRef.refField} " +
                                        " where sc_amount.audit_state = 'A' and sc_comp.${scaleComponentRef.refField} = $scaleComponentId " +
                                        filterObjCond +
                                        " order by sc_amount.id "

                                if (objectEvent == "Read") {
                                    taskFileFields = mutableListOf<TaskFileFields>()
                                    //readOneObject(taskObject, scaleAmountClass, jsonConfigFile, false, sqlQuery)
                                    readOneObject(taskObject, scaleAmountClass, jsonConfigFile, sqlQuery, 0)
                                } else if (objectEvent == "Load") {

                                    // проверка scaleObjects
                                    val isAddScaleObjects =
                                        compareScaleObjects(
                                            sqlQuery,
                                            scaleAmountClass,
                                            scaleObjects,
                                            oneConfigClass,
                                            tblFieldsOneObj
                                        )
                                    if (!isAddScaleObjects) {
                                        return false
                                    }
                                }
                            }
                        }
                    }
                }
            }
            //}
        }
        return true
    }

    /* НЕ УДАЛЯТЬ*/
    // выгрузка linkObject.keyType=In. объекты linkObject.keyType=In выгружаются как главные объекты
    // в первую очередь сделано для выгрузки всех тарифов для тарифной группы, указанной в файле заданий с "loadMode": "Safe.linkobjects"
    private fun readLinkObjectIn(     /* НЕ УДАЛЯТЬ*/
        oneTaskObject: TaskFileFields,
        oneConfigClass: ObjectCfg, // класс из конфигурации
        jsonConfigFile: RootCfg,
        oneLinkObjectIn: LinkObjects // референс linkObjects.keyType=In
    ) {

        /*
        При выгрузке данных может возникнуть необходимость выгрузить все(или все измененные) объекты определенного типа.
        Например, все тарифы относящиеся к указанной тарифной группе. Если формировать файл задания в режиме -dc получаем все измененные объекты
        описанные в файле конфигурации. В некоторых случаях может быть неудобно.
        Предлагается:
        анализировать связь linkObjects.keyType=In, только в случае указания в файле задания режима "loadMode": "Safe.linkObjects"
        При выгрузке данных анализировать настройку и для "keyType": "In" в linkObjects выгружать все объекты найденные по этой связи как основные объекты.
        В данном случае это все связанные с объектом тарифы. Подчиненные по связи "keyType": "InGroup" выгружаем как вложенные.
        Проверка по дате стандартная для файла задания.
        */

        var sqlQuery = ""
        val aliasTableLinkObj = "trf"
        val aliasTableLinkObjDop = "trf_dop"

        // класс объекта oneLinkObjectIn
        val oneLinkObjectInClass =
            jsonConfigFile.objects[jsonConfigFile.objects.indexOfFirst { it.code == oneLinkObjectIn.codeRef }]

        val tableName = oneLinkObjectInClass.tableName

        // формирование запроса для поиска всех linkObjects.keyType="In" для главного объекта oneTaskObject
        val auditDateObjCond =
            " and $aliasTableLinkObj.${oneLinkObjectInClass.auditDateField} > to_timestamp(' $AUDIT_DATE ','YYYY-MM-DD') "
        var filterObjCond = ""
        if (oneLinkObjectInClass.filterObjects != "") {
            filterObjCond = " and ${oneLinkObjectInClass.filterObjects} "
        }

        val mainObjId = oneTaskObject.keyFieldIn.find { it.fieldName == oneConfigClass.keyFieldIn }!!.fieldValue

        val linkFieldForMainTable = oneLinkObjectIn.refField
        val dopCondForMainObjId = " and $aliasTableLinkObj.$linkFieldForMainTable=$mainObjId "

        if (oneConfigClass.linkObjects.filter { it.keyType.lowercase() == "in" }.isNotEmpty()
        ) {
            sqlQuery =
                "\nselect ${oneConfigClass.keyFieldIn},${oneConfigClass.keyFieldOut} from  $tableName $aliasTableLinkObj where audit_state = 'A' $filterObjCond $dopCondForMainObjId and ${oneConfigClass.keyFieldIn} in (\n"
            sqlQuery += "select ${oneConfigClass.keyFieldIn} from $tableName $aliasTableLinkObj where audit_state = 'A' $auditDateObjCond $filterObjCond $dopCondForMainObjId \n"

            for (oneCfgLinkObj in oneLinkObjectInClass.linkObjects.filter { it.keyType.lowercase() == "ingroup" }) {

                val oneCfgLinkObjClass = jsonConfigFile.objects.find { it.code == oneCfgLinkObj.codeRef }!!

                var filterObjCondLink = ""
                if (oneCfgLinkObjClass.filterObjects != "") {
                    filterObjCondLink = " and ${oneCfgLinkObjClass.filterObjects} "
                }

                val auditDAteObjCondLink =
                    " and $aliasTableLinkObjDop.${oneCfgLinkObjClass.auditDateField} > to_timestamp(' $AUDIT_DATE ','YYYY-MM-DD') "

                sqlQuery += "union \n select $aliasTableLinkObjDop.${oneCfgLinkObj.refField} \n" +
                        "  from ${oneCfgLinkObjClass.tableName} $aliasTableLinkObjDop \n" +
                        "  join $tableName $aliasTableLinkObj on $aliasTableLinkObj.${oneConfigClass.keyFieldIn}=$aliasTableLinkObjDop.${oneCfgLinkObj.refField} $dopCondForMainObjId \n" +
                        " where $aliasTableLinkObjDop.audit_state = 'A' $auditDAteObjCondLink $filterObjCondLink \n "
            }
            sqlQuery += ") " + "order by id"
        }

        logger.trace(
            CommonFunctions().createObjectIdForLogMsg(
                oneConfigClass.code,
                oneConfigClass.keyFieldIn,
                oneTaskObject.keyFieldIn,
                -1
            ) + "Query to the linkObject with keyType=In, class <${oneLinkObjectInClass.code}>: $sqlQuery"
        )

        //поиск объектов linkObject.keyType=In для выгрузки
        val queryFieldValue = conn.prepareStatement(sqlQuery)
        val resultQuery = queryFieldValue.executeQuery()

        while (resultQuery.next()) {

            val tblFields = mutableListOf<Fields>()
            for (i in 1..resultQuery.metaData.columnCount) {
                // название поля таблицы
                val fieldName = resultQuery.metaData.getColumnName(i)
                // исключение из результирующего массива не нужных полей
                if (oneConfigClass.fieldsNotExport.find { it.name == fieldName } != null) {
                    continue
                }
                // значение поля таблицы
                val fieldValue = resultQuery.getString(i)
                tblFields.add(Fields(fieldName, fieldValue))
            }
            val lstKeyFieldInValue =
                getOneFieldFromRefField(tblFields, oneLinkObjectInClass, oneLinkObjectInClass.keyFieldIn)
            val lstKeyFieldOutValue =
                getOneFieldFromRefField(tblFields, oneLinkObjectInClass, oneLinkObjectInClass.keyFieldOut)
            val taskObject =
                TaskFileFields(oneLinkObjectInClass.code, "Safe", lstKeyFieldInValue, lstKeyFieldOutValue)

            //readOneObject(taskObject, oneLinkObjectInClass, jsonConfigFile, false, "")
            readOneObject(taskObject, oneLinkObjectInClass, jsonConfigFile, "", 0)
        }
        queryFieldValue.close()
    }

    private fun compareScaleObjects(
        sqlQuery: String,
        scaleClass: ObjectCfg,
        scaleObjects: List<DataDB>,
        linkConfigClass: ObjectCfg,  // класс linkObject
        tblFieldsOneObj: List<Fields>
    ): Boolean {

        val scaleObjectFieldsFromDB = mutableListOf<List<Fields>>()
        var scaleOneObjectFieldsFromDB = mutableListOf<Fields>()

        var iRowCount = 0 // кол-во записей класса scaleConfigClass в БД
        val connCompare = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
        logger.trace(
            CommonFunctions().createObjectIdForLogMsg(
                linkConfigClass.code,
                linkConfigClass.keyFieldIn,
                tblFieldsOneObj,
                -1
            ) + "Query to scaleObjects <${scaleClass.code}> : $sqlQuery"
        )

        val scaleIdFieldName = scalable.getScaleIdFieldName(scaleClass)
        val scaleAmountClassName = scalable.scalableAmountClassName
        val scaleCompFieldName = scalable.scaleComponentValueFieldName

        val queryStatement = connCompare.prepareStatement(sqlQuery)
        val queryResult = queryStatement.executeQuery()
        while (queryResult.next()) {

            // формирую список полей и значений для одной записи, которую нужно проверить
            for (i in 1..queryResult.metaData.columnCount) {
                // название поля таблицы
                val fieldName = queryResult.metaData.getColumnName(i)
                // значение поля таблицы
                val fieldValue = queryResult.getString(i)

                // для объектов scalableamount проверяю на совпадение кроме прочего еще и значение поля scale_id
                if (scaleClass.fieldsNotExport.find { it.name == fieldName } == null &&
                    ((scaleClass.scale!!.find { it.refField == fieldName } == null) ||
                            //((fieldName.lowercase() == CommonConstants().SCALE_ID_FIELD_NAME) && scaleClass.code.lowercase() == CommonConstants().SCALE_AMOUNT_CLASS_NAME))
                            ((fieldName.lowercase() == scaleIdFieldName) && scaleClass.code.lowercase() == scaleAmountClassName))

                ) {
                    scaleOneObjectFieldsFromDB.add(Fields(fieldName, fieldValue))
                }
            }
            scaleObjectFieldsFromDB.add(scaleOneObjectFieldsFromDB)
            scaleOneObjectFieldsFromDB = mutableListOf<Fields>()
            iRowCount++
        }
        connCompare.close()

        var scaleObjectFromId = ""
        var scaleObjectFromFileFieldName = ""

        // каждую запись из файла сравниваю с записью из БД
        var isRowEqual = true
        // цикл по объектам файла
        for (oneScaleObject in scaleObjects.filter { it.code == scaleClass.code }) {
            // цикл по объектам БД
            for (fieldsFromDB in scaleObjectFieldsFromDB) {

                isRowEqual = true
                // цикл по полям строки файла
                for ((fieldNameFromFile, fieldValueFromFile) in oneScaleObject.row.fields) {

                    // значение каждого поля из файла сравниваю со значением такого же поля из БД
                    if (fieldNameFromFile != scaleClass.keyFieldIn &&
                        fieldNameFromFile.lowercase() != scaleCompFieldName &&
                        ((scaleClass.scale!!.find { it.refField == fieldNameFromFile } == null)
                                //|| (fieldNameFromFile.lowercase() == CommonConstants().SCALE_ID_FIELD_NAME) && scaleClass.code.lowercase() == CommonConstants().SCALE_AMOUNT_CLASS_NAME) &&
                                || (fieldNameFromFile.lowercase() == scaleIdFieldName) && scaleClass.code.lowercase() == scaleAmountClassName) &&
                        fieldsFromDB.find { it.fieldName == fieldNameFromFile }!!.fieldValue != fieldValueFromFile
                    ) {
                        if (scaleObjectFromId == "") {
                            scaleObjectFromId =
                                oneScaleObject.row.fields.find { it.fieldName == scaleClass.keyFieldIn }!!.fieldValue!!
                            scaleObjectFromFileFieldName = fieldNameFromFile
                        }
                        //если значение поля из файла не совпало со значением из БД, то поиск по следующей строке из БД
                        isRowEqual = false
                        break
                    }
                }
                // если строка из файла совпала со строкой из БД, то переходим к проверке следующей строки из файла
                if (isRowEqual) {
                    break
                }
            }
            // если после сравнения строки из файла isRowEqual=false, то в БД не нашлось соответствия. дальнейшие проверки не нужны
            if (!isRowEqual) {
                break
            }
        }

        if (!isRowEqual) {
            logger.debug(
                CommonFunctions().createObjectIdForLogMsg(
                    linkConfigClass.code,
                    linkConfigClass.keyFieldIn,
                    tblFieldsOneObj,
                    -1
                ) + "The scaleObject <${scaleClass.code}> from file did not match with the scaleObject receiver database." +
                        "The file scaleObject ID <$scaleObjectFromId>. " +
                        "The difference in the column <$scaleObjectFromFileFieldName>."
            )
            return false
        }

        // каждую запись из БД сравниваю с записью из файла
        isRowEqual = true
        scaleObjectFromId = ""
        // цикл по объектам БД
        for (fieldsFromDB in scaleObjectFieldsFromDB) {
            // цикл по объектам файла
            for (oneScaleObject in scaleObjects.filter { it.code == scaleClass.code }) {

                isRowEqual = true
                // цикл по полям строки файла
                for ((fieldNameFromDB, fieldValueFromDB) in fieldsFromDB) {

                    // значение каждого поля из БД сравниваю со значением такого же поля из файла
                    if (fieldNameFromDB != scaleClass.keyFieldIn &&
                        fieldNameFromDB.lowercase() != scaleCompFieldName &&
                        ((scaleClass.scale!!.find { it.refField == fieldNameFromDB } == null)
                                //|| (fieldNameFromDB.lowercase() == CommonConstants().SCALE_ID_FIELD_NAME) && scaleClass.code.lowercase() == CommonConstants().SCALE_AMOUNT_CLASS_NAME) &&
                                || (fieldNameFromDB.lowercase() == scaleIdFieldName) && scaleClass.code.lowercase() == scaleAmountClassName) &&
                        oneScaleObject.row.fields.find { it.fieldName == fieldNameFromDB }!!.fieldValue != fieldValueFromDB
                    ) {
                        if (scaleObjectFromId == "") {
                            scaleObjectFromId =
                                fieldsFromDB.find { it.fieldName == scaleClass.keyFieldIn }!!.fieldValue!!
                            scaleObjectFromFileFieldName = fieldNameFromDB
                        }
                        //если значение поля из БД не совпало со значением из файла, то поиск по следующей строке из файла
                        isRowEqual = false
                        break
                    }
                }
                // если строка из БД совпала со строкой из файла, то переходим к проверке следующей строки из БД
                if (isRowEqual) {
                    break
                }
            }
            // если после сравнения строки из БД isRowEqual=false, то в файле не нашлось соответствия
            if (!isRowEqual) {
                break
            }
        }

        if (!isRowEqual) {
            logger.debug(
                CommonFunctions().createObjectIdForLogMsg(
                    linkConfigClass.code,
                    linkConfigClass.keyFieldIn,
                    tblFieldsOneObj,
                    -1
                ) + "The scaleObject <${scaleClass.code}> from receiver database did not match with the scaleObject from file." +
                        "The receiver database scaleObject ID <$scaleObjectFromId>. " +
                        "The difference in the column <$scaleObjectFromFileFieldName>."
            )
            return false
        }

        if (iRowCount != scaleObjects.count { it.code == scaleClass.code }) {

            logger.debug(
                CommonFunctions().createObjectIdForLogMsg(
                    linkConfigClass.code,
                    linkConfigClass.keyFieldIn,
                    tblFieldsOneObj,
                    -1
                ) + "Number of scaleObjects <${scaleClass.code}> links did not match for class ${linkConfigClass.code}"
            )
            return false
        }

        logger.debug(
            CommonFunctions().createObjectIdForLogMsg(
                linkConfigClass.code,
                linkConfigClass.keyFieldIn,
                tblFieldsOneObj,
                -1
            ) + "The scaleObjects <${scaleClass.code}> match."
        )
        return true
    }

}