package com.solanteq.service

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import kotlin.system.exitProcess

//class CheckObject(allCheckObjectMain: DataDBMain) {
object CheckObject {

    //private val allCheckObject = allCheckObjectMain
    public lateinit var allCheckObject: DataDBMain

    // действие при загрузке для каждого объекта из файла: добавление/обновление/пропуск
    public lateinit var listOfActionWithObject: MutableList<ActionWithObject>

    private val logger = LoggerFactory.getLogger(javaClass)

    private val createReport = CreateReport()

    private val readerDB = ReaderDB()

    private val loadObject = LoadObject

    //public var allCheckObject = DataDBMain(listOf<CfgList>(), listOf<DataDB>())

    // считывание конфигурации
    private val readJsonFile = ReadJsonFile()
    private val jsonConfigFile = readJsonFile.readConfig()
    //public var jsonConfigFile = RootCfg(listOf<CfgList>(), listOf<ObjectCfg>())

    private val scalable = Scalable(jsonConfigFile)

    // коннект к БД
    private lateinit var conn: Connection

    private var listNewScalableAmountObject = mutableListOf<ScaleAmountInDB>()

    //fun checkDataObject(listOfActionWithObject: MutableList<ActionWithObject>) {
    fun checkDataObject() {


        //val jsonConfigFile1 = configJson.readJsonFile<RootCfg>(CONFIG_FILE)
        logger.info("Configuration info: " + jsonConfigFile.cfgList[0])

        //val scalable = Scalable(jsonConfigFile)

        // формирование списка полей, которые не нужно выгружать, для каждого класса
        CommonFunctions().createListFieldsNotExport(jsonConfigFile)

        // считывание файла объектов
        //allCheckObject = readJsonFile.readObject()

        // проверка версии и названия конфигурационного файла и файла с объектами
        if (jsonConfigFile.cfgList[0].version != allCheckObject.cfgList[0].version ||
            jsonConfigFile.cfgList[0].cfgName != allCheckObject.cfgList[0].cfgName
        ) {
            logger.error("Different versions or cfgName of Configuration File and Object File.")
            exitProcess(-1)
        }

        // Проверка наличия класса шкалы в файле конфигурации.
        // Если в импортируемом файле есть не пустая структура scaleObjects, а в файле конфигурации нет класса шкалы, то ошибка.
        if (scalable.getClassNameByScaleKeyType("InScale") == "" && allCheckObject.element.find { it.row.linkObjects.find { linkObjects -> linkObjects.row.scaleObjects.isNotEmpty() } != null } != null) {
            logger.error("There is no description of scale class in configuration file.")
            exitProcess(-1)
        }

        // установка соединения с БД
        conn = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)

        // проверка цикличности ссылок
        // цикл по главным объектам (главные объекты это объекты, которые нужно загрузить в БД).
        for (oneCheckObject in allCheckObject.element) {
            // поиск описания класса референсного объекта в файле конфигурации
            var oneConfCheckObj = jsonConfigFile.objects.find { it.code == oneCheckObject.code }!!
            val chainCheckObject = mutableListOf<DataDB>(oneCheckObject)
            checkRingReference(chainCheckObject, oneConfCheckObj, jsonConfigFile, allCheckObject.element)

            // цикл по объектам linkObjects
            for (oneCheckLinkObj in oneCheckObject.row.linkObjects) {
                oneConfCheckObj = jsonConfigFile.objects.find { it.code == oneCheckLinkObj.code }!!
                checkRingReference(chainCheckObject, oneConfCheckObj, jsonConfigFile, allCheckObject.element)

                // цикл по объектам scaleObjects
                oneCheckLinkObj.row.scaleObjects.let { scaleObjects ->
                    for (oneCheckScaleObj in scaleObjects) {
                        oneConfCheckObj = jsonConfigFile.objects.find { it.code == oneCheckScaleObj.code }!!
                        checkRingReference(chainCheckObject, oneConfCheckObj, jsonConfigFile, allCheckObject.element)
                    }
                }
            }
        }

        // цикл по объектам. вторая часть проверок
        for (oneCheckObject in allCheckObject.element) {

            checkOneObject(oneCheckObject, jsonConfigFile, allCheckObject.element)

            // цикл по объектам linkObjects
            for (oneCheckLinkObj in oneCheckObject.row.linkObjects) {
                checkOneObject(oneCheckLinkObj, jsonConfigFile, allCheckObject.element)

                // цикл по объектам scaleObjects
                oneCheckLinkObj.row.scaleObjects.let { scaleObjects ->
                    for (oneCheckScaleObj in scaleObjects) {
                        checkOneObject(oneCheckScaleObj, jsonConfigFile, allCheckObject.element)
                    }
                }
            }

            //////////////////// Установка значений рефернсов для Главного объекта ////////////////////
            // поиск описания класса объекта, который нужно проверить, в файле конфигурации

            var actionInsert: Boolean = false
            var actionUpdateMainRecord: Boolean = false
            var actionUpdateRefTables: Boolean = false
            var actionUpdateLinkRecord: Boolean = false
            var actionSkip: Boolean = false

            listNewScalableAmountObject = mutableListOf<ScaleAmountInDB>()

            val oneConfClassMainObj = jsonConfigFile.objects.find { it.code == oneCheckObject.code }!!

            // установка нового значения ссылочного поля в файле для референса типа fieldJson
            //LoadObject(allCheckObject).setNewIdFieldJson(
            setNewIdFieldJson(
                oneCheckObject.row.fields,
                oneCheckObject.row.refObject,
                oneConfClassMainObj,
                jsonConfigFile
            )
            for (oneLinkObject in oneCheckObject.row.linkObjects) {
                var oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneLinkObject.code }!!
                //LoadObject(allCheckObject).setNewIdFieldJson(
                setNewIdFieldJson(
                    oneLinkObject.row.fields,
                    oneLinkObject.row.refObject,
                    oneConfClassRefObj,
                    jsonConfigFile
                )
                for (oneScaleObject in oneLinkObject.row.scaleObjects) {
                    oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneScaleObject.code }!!
                    //LoadObject(allCheckObject).setNewIdFieldJson(
                    setNewIdFieldJson(
                        oneScaleObject.row.fields,
                        oneScaleObject.row.refObject,
                        oneConfClassRefObj,
                        jsonConfigFile
                    )
                }
            }

            // установка нового значения ссылочного поля в файле для референса типа refFields
            //LoadObject(allCheckObject).setNewIdRefFields(
            setNewIdRefFields(
                oneCheckObject.row.fields,
                oneCheckObject.row.refObject,
                oneConfClassMainObj,
                jsonConfigFile,
                null
            )
            for (oneLinkObject in oneCheckObject.row.linkObjects) {
                var oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneLinkObject.code }!!
                //LoadObject(allCheckObject).setNewIdRefFields(
                setNewIdRefFields(
                    oneLinkObject.row.fields,
                    oneLinkObject.row.refObject,
                    oneConfClassRefObj,
                    jsonConfigFile,
                    oneCheckObject
                )
                for (oneScaleObject in oneLinkObject.row.scaleObjects) {
                    oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneScaleObject.code }!!
                    // LoadObject(allCheckObject).setNewIdRefFields(
                    setNewIdRefFields(
                        oneScaleObject.row.fields,
                        oneScaleObject.row.refObject,
                        oneConfClassRefObj,
                        jsonConfigFile,
                        oneCheckObject
                    )
                }
            }

            // поиск основного объекта в базе приемнике.
            // должен быть найден ровно один объект
            val idObjectInDB = findObjectInDB(oneCheckObject.row.fields, oneConfClassMainObj, -1)
            //val idObjectInDB = LoadObject.getObjIdInDB(oneCheckObject.row.fields, oneConfClassMainObj, 0, false)

            // установка нового значения шкалы
            // идентификатор шкалы
            var idScaleInDB = ""

            // если у класса есть шкала, то работаем с ней
            if (scalable.isClassHaveScale(oneConfClassMainObj)) {

                // знаю ид тарифа в БД приемнике. ищу связанную с ним шкалу
                if (idObjectInDB != "-") {
                    idScaleInDB = loadObject.findScaleIdInDB(oneConfClassMainObj, idObjectInDB)
                }

                // если закачиваемого тарифа нет в БД приемнике, то проверяю есть ли шкала в тарифе в файле данных и если есть,
                //   то генерю новый ид шкалы и генерю запрос порождающий новую шкалу в БД приемнике
                if (idScaleInDB == "") {
                    idScaleInDB = getNewScaleId(oneCheckObject, oneConfClassMainObj/*, jsonConfigFile*/)
                }

                // установка значения идентификатора шкалы в файле
                if (idScaleInDB != "") {
                    setNewIdScale(oneCheckObject, idScaleInDB)
                }
            }

            // запросы для обновления/добавления linkObject в БД
            // используется при загрузке данных(load)
            var sqlQueryLinkObject = ""
            var sqlQueryObjDeclare = ""
            var sqlQueryObjInit = ""

            // проверки основного объекта
            if (idObjectInDB != "-") {

                // Сравнение значения значимых полей из файла со значениями колонок в таблице базы приемника.
                // Значимые поля это поля объекта из файла, которые не являются ссылками в списке refObjects
                val differentFieldName =
                    checkFieldValue(oneCheckObject.row.fields, oneConfClassMainObj, idObjectInDB)
                if (differentFieldName != "") {
                    logger.debug(
                        CommonFunctions().createObjectIdForLogMsg(
                            oneConfClassMainObj.code,
                            oneConfClassMainObj.keyFieldIn,
                            oneCheckObject.row.fields,
                            1
                        ) + "The main object does not match with the object from receiver database. " +
                                "The database object ID <$idObjectInDB>. The difference in the column <$differentFieldName>."
                    )
                    actionUpdateMainRecord = true
                }

                // сравнение ссылок refTables
                if (!compareObjectRefTables(
                        idObjectInDB,
                        oneConfClassMainObj,
                        oneCheckObject.row.refObject,
                        oneCheckObject.row.fields,
                        jsonConfigFile
                    )
                ) {
                    logger.debug(
                        CommonFunctions().createObjectIdForLogMsg(
                            oneConfClassMainObj.code,
                            oneConfClassMainObj.keyFieldIn,
                            oneCheckObject.row.fields,
                            1
                        ) + "The main object does not match with the object from receiver database. " +
                                "RefTables links of main object don't match with object from DB."
                    )
                    actionUpdateRefTables = true

                }

            } else {
                actionInsert = true
            }

            // проверка linkObject и формирование запроса на изменение объектов linkObjects
            val listQueryCondition =
                //LoadObject.createLinkObjectsQuery(
                createLinkObjectsQuery(
                    oneCheckObject,
                    oneConfClassMainObj,
                    jsonConfigFile,
                    idObjectInDB
                )
            sqlQueryObjDeclare += listQueryCondition[0]
            sqlQueryObjInit += listQueryCondition[1]
            sqlQueryLinkObject += listQueryCondition[2]
            if (sqlQueryLinkObject != "") {
                actionUpdateLinkRecord = true
            }

            // если объект не является новым и не изменился, значит он есть в базе и полностью соответствует объекту из файла
            if (!actionInsert && !actionUpdateMainRecord && !actionUpdateRefTables && !actionUpdateLinkRecord) {
                actionSkip = true
            }

            val idObjectFromFile =
                oneCheckObject.row.fields.find { it.fieldName == oneConfClassMainObj.keyFieldIn }!!.fieldValue!!

            val actionWithObject = ActionWithObject(
                idObjectFromFile,
                oneConfClassMainObj.code,
                actionInsert,
                actionUpdateMainRecord,
                actionUpdateRefTables,
                actionUpdateLinkRecord,
                sqlQueryObjDeclare,
                sqlQueryObjInit,
                sqlQueryLinkObject,
                actionSkip
            )

            listOfActionWithObject.add(actionWithObject)

            logger.info(
                CommonFunctions().createObjectIdForLogMsg(
                    oneConfClassMainObj.code,
                    oneConfClassMainObj.keyFieldOut,
                    oneCheckObject.row.fields,
                    -1
                ) + if (actionInsert)
                    "The object will be inserted into the database during loading. "
                else if (actionUpdateMainRecord || actionUpdateRefTables || actionUpdateLinkRecord)
                    "The object will be updated in the database during loading. "
                else
                    "The object will be skipped during loading."
            )

            createReport.createReportOfProgramExecution(oneConfClassMainObj.code, actionWithObject)

        }
        conn.close()

        createReport.writeReportOfProgramExecution("check")

    }

    // проверка одного объекта
    private fun checkOneObject(
        oneCheckMainObj: DataDB,
        jsonConfigFile: RootCfg,
        allCheckObject: List<DataDB>,
    ) {

        // поиск описания класса объекта, который нужно проверить, в файле конфигурации
        val oneConfClassMainObj = jsonConfigFile.objects.find { it.code == oneCheckMainObj.code }

        if (oneConfClassMainObj != null) {

            // сравнение названия полей объекта из файла с названиями полей таблицы его класса из базы приемника
            checkFieldsName(oneCheckMainObj.row.fields, oneConfClassMainObj, 0)

            // проверка референсного объекта
            val allCheckRefObj = oneCheckMainObj.row.refObject
            for (oneCheckRefObj in allCheckRefObj) {

                // класс референсного объекта
                val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneCheckRefObj.code }!!

                // проверка референсного объекта
                checkOneRefObject(oneCheckRefObj, allCheckObject, oneConfClassRefObj, jsonConfigFile)

                // проверка референсных объектов 2 уровня
                for (oneCheckRefObj2Lvl in oneCheckRefObj.refObject) {
                    val oneConfClassRefObj2Lvl = jsonConfigFile.objects.find { it.code == oneCheckRefObj2Lvl.code }!!
                    checkOneRefObject(oneCheckRefObj2Lvl, allCheckObject, oneConfClassRefObj2Lvl, jsonConfigFile)
                }
            }

        } else {
            logger.error(
                CommonFunctions().createObjectIdForLogMsg(oneCheckMainObj.code, "", oneCheckMainObj.row.fields, 0) +
                        "The class was not found in the configuration file."
            )
            exitProcess(-1)
        }
    }

    // проверка референсного объекта
    private fun checkOneRefObject(
        oneCheckRefObj: RefObject,
        allMainObj: List<DataDB>,
        oneConfClassRefObj: ObjectCfg,
        jsonConfigFile: RootCfg
    ) {

        val keyFieldIn = oneConfClassRefObj.keyFieldIn
        // идентификатор референсного объекта
        val keyFieldInValue = oneCheckRefObj.row.fields.find { it.fieldName == keyFieldIn }!!.fieldValue

        // ссылки на шкалу пропускаю, т.к. в них только id самой шкалы: проверять нечего
        if (oneCheckRefObj.typeRef.lowercase() == "inparentscale" || oneCheckRefObj.typeRef.lowercase() == "inscale") {
            return
        }

        if (oneCheckRefObj.nestedLevel == 2) {
            ReaderDB().checkFldOutForLink(
                oneConfClassRefObj,
                oneCheckRefObj.row.fields,
                oneConfClassRefObj.keyFieldOut/*, "keyFieldOut"*/
            )
        }

        // ищу референсный объект среди главных объектов в файле и сравниваю их поля(сравнение идет по названиям полей в файле)
        // если главный объект найден и его поля совпадают с полями референсного объекта, то проверка пройдена успешно
        // для референса уровня 1 сверка значений всех полей, для референса уровня 2 сверка только значения поля keyFieldIn
        for (item in allMainObj.filter { it.code == oneConfClassRefObj.code }) {
            if (item.row.fields.find { it.fieldName == keyFieldIn && it.fieldValue == keyFieldInValue } != null) {
                if ((item.row.fields == oneCheckRefObj.row.fields && oneCheckRefObj.nestedLevel == 1)
                    || (item.row.fields.contains(Fields(keyFieldIn, keyFieldInValue))
                            && oneCheckRefObj.nestedLevel == 2)
                ) {
                    logger.debug(
                        CommonFunctions().createObjectIdForLogMsg(
                            oneConfClassRefObj.code,
                            oneConfClassRefObj.keyFieldOut,
                            oneCheckRefObj.row.fields,
                            oneCheckRefObj.nestedLevel
                        ) + "The object was found among the main objects."
                    )
                    // проверка пройдена успешно
                    return
                }
                logger.debug(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneConfClassRefObj.code,
                        oneConfClassRefObj.keyFieldOut,
                        oneCheckRefObj.row.fields,
                        oneCheckRefObj.nestedLevel
                    ) + "The object was not found among the main objects."
                )
            }
        }

        // поиск референсного объекта в базе приемнике.
        // должен быть найден ровно один объект
        val idObjectInDB = findObjectInDB(oneCheckRefObj.row.fields, oneConfClassRefObj, oneCheckRefObj.nestedLevel)

        // проверка пройдена успешно для референса 2 уровня
        if (idObjectInDB != "-" && oneCheckRefObj.nestedLevel == 2) {
            return
        }

        if (oneCheckRefObj.nestedLevel == 1) {

            // сравнение названия полей референсного объекта из файла с названиями полей таблицы его класса из базы приемника
            checkFieldsName(oneCheckRefObj.row.fields, oneConfClassRefObj, oneCheckRefObj.nestedLevel)

            // установка нового значения ссылочного поля в файле для референса типа fieldJson
            //LoadObject(allCheckObject).setNewIdFieldJson(
            setNewIdFieldJson(
                oneCheckRefObj.row.fields,
                oneCheckRefObj.refObject,
                oneConfClassRefObj,
                jsonConfigFile
            )

            // установка нового значения ссылочного поля в файле для референса типа refFields
            //LoadObject(allCheckObject).setNewIdRefFields(
            setNewIdRefFields(
                oneCheckRefObj.row.fields,
                oneCheckRefObj.refObject,
                oneConfClassRefObj,
                jsonConfigFile,
                null
            )

            if (!compareObjectRefTables(
                    idObjectInDB,
                    oneConfClassRefObj,
                    oneCheckRefObj.refObject,
                    oneCheckRefObj.row.fields,
                    jsonConfigFile
                )
            ) {
                logger.error(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneConfClassRefObj.code,
                        oneConfClassRefObj.keyFieldIn,
                        oneCheckRefObj.row.fields,
                        1
                    ) + "<RefTables links of reference object don't match with object from DB. "
                )
                exitProcess(-1)
            }

            // сравнение значения значимых полей из файла со значениями колонок в таблице базы приемника.
            // значимые поля это поля объекта из файла, которые не являются ссылками в списке refObjects
            checkRefFieldValue(
                oneCheckRefObj.row.fields,
                oneConfClassRefObj,
                oneCheckRefObj.nestedLevel,
                idObjectInDB
            )
        }
    }

    // поиск объекта в базе приемнике
    fun findObjectInDB(
        objFieldsList: List<Fields>,
        oneConfigClass: ObjectCfg,
        nestedLevel: Int
    ): String {

        var idObjectInDB = "-"

        if (oneConfigClass.keyFieldOut == "") {
            logger.error(
                CommonFunctions().createObjectIdForLogMsg(
                    oneConfigClass.code,
                    oneConfigClass.keyFieldOut,
                    objFieldsList,
                    nestedLevel
                ) + "<" + oneConfigClass.keyFieldIn + "=" + objFieldsList.find { it.fieldName == oneConfigClass.keyFieldIn }!!.fieldValue + ">." +
                        " The " + if (nestedLevel > -1) {
                    "reference"
                } else {
                    ""
                } + " object class has an empty keyFieldOut, so this " + if (nestedLevel > -1) {
                    "reference"
                } else {
                    ""
                } + " object cannot be found in receiver database."
            )

            exitProcess(-1)
        }

        // запрос на поиск объекта
        val sqlQuery = createSqlQueryToFindObj(objFieldsList, oneConfigClass)
        logger.trace(
            CommonFunctions().createObjectIdForLogMsg(
                oneConfigClass.code,
                oneConfigClass.keyFieldOut,
                objFieldsList,
                nestedLevel
            ) + "Query to the checked object: $sqlQuery."
        )

        val connFindObjId = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
        val queryStatement = connFindObjId.prepareStatement(sqlQuery)
        val queryResult = queryStatement.executeQuery()

        // объект из файла найден в базе
        if (queryResult.isBeforeFirst) {
            queryResult.next()
            // найден только один объект
            if (queryResult.getInt(2) == 1) {
                idObjectInDB = queryResult.getString(1)
                logger.debug(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneConfigClass.code,
                        oneConfigClass.keyFieldOut,
                        objFieldsList,
                        nestedLevel
                    ) + "The object was found. It has ${oneConfigClass.keyFieldIn}=$idObjectInDB."
                )
            } else { // найдено больше одного объекта
                logger.error(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneConfigClass.code,
                        oneConfigClass.keyFieldOut,
                        objFieldsList,
                        nestedLevel
                    ) + "More than one object was found in the receiver database."
                )
                connFindObjId.close()
                exitProcess(-1)
            }
        } else if (nestedLevel == 0) { // главный объект из файла НЕ найден в базе
            logger.debug(
                CommonFunctions().createObjectIdForLogMsg(
                    oneConfigClass.code,
                    oneConfigClass.keyFieldOut,
                    objFieldsList,
                    nestedLevel
                ) + "The object was not found. It will be add to the receiver database."
            )
        } else if (nestedLevel > 0) { // референсный объект из файла НЕ найден в базе
            logger.error(
                CommonFunctions().createObjectIdForLogMsg(
                    oneConfigClass.code,
                    oneConfigClass.keyFieldOut,
                    objFieldsList,
                    nestedLevel
                ) + "The reference object was not found."
            )
            connFindObjId.close()
            exitProcess(-1)
        }
        queryResult.close()
        connFindObjId.close()
        return idObjectInDB
    }

    // формирование запроса для поиска объекта в базе приемнике
    public fun createSqlQueryToFindObj(
        objFieldsList: List<Fields>,
        oneConfigClass: ObjectCfg
    ): String {
        val sqlQuery: String

        var conditionSql = ""
        val tableName = oneConfigClass.tableName
        val fieldName = oneConfigClass.keyFieldOut

        val arrRefField = fieldName.split(",")
        // поля таблицы в keyFieldOut могут быть перечислены через запятую
        for (i in arrRefField.indices) {
            if (i > 0) {
                conditionSql += " and "
            }
            conditionSql += "(" + arrRefField[i] + "='" + objFieldsList.find { it.fieldName == arrRefField[i] }!!.fieldValue + "')"
        }

        if (oneConfigClass.filterObjects != "") {
            conditionSql += " and ${oneConfigClass.filterObjects} "
        }

        sqlQuery = "select ${oneConfigClass.keyFieldIn}, count(id) over() " +
                "from $tableName " +
                "where $conditionSql and audit_state = 'A'"

        return sqlQuery
    }

    // сравнение названия полей объекта из файла с названиями полей таблицы его класса из базы приемника
    private fun checkFieldsName(
        //oneCheckObject: DataDB,
        objFieldsList: List<Fields>,
        oneConfigClass: ObjectCfg,
        nestedLevel: Int
    ) {

        val sqlQuery = "select column_name " +
                "from information_schema.columns " +
                "where table_name = '${oneConfigClass.tableName}'"
        logger.trace(
            CommonFunctions().createObjectIdForLogMsg(oneConfigClass.code, "", objFieldsList, nestedLevel) +
                    "Query to the class table to checked column name <${oneConfigClass.tableName}>: $sqlQuery."
        )
        val queryStatement = conn.prepareStatement(sqlQuery)
        val queryResult = queryStatement.executeQuery()

        // формирование массива из ResultSet
        val listColumnName = queryResult.use {
            generateSequence {
                if (queryResult.next()) queryResult.getString(1).lowercase() else null
            }.toList()
        }

        // сравнение названий колонок таблицы из базы с названиями полей объекта в файле
        for (columnName in listColumnName) {
            if (objFieldsList.find { it.fieldName == columnName } == null &&
                oneConfigClass.fieldsNotExport.find { it.name == columnName } == null) {
                logger.error(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneConfigClass.code,
                        oneConfigClass.keyFieldOut,
                        objFieldsList,
                        nestedLevel
                    ) + "Not find column <$columnName> from table <${oneConfigClass.tableName}> in the Object File fields."
                )
                exitProcess(-1)
            }
        }

        // сравнение названий полей объекта в файле с названиями колонок таблицы в базе
        for ((columnName, columnValue) in objFieldsList) {
            if (!listColumnName.contains(columnName) && oneConfigClass.fieldsNotExport.find { it.name == columnName } == null) {
                logger.error(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneConfigClass.code,
                        oneConfigClass.keyFieldOut,
                        objFieldsList,
                        nestedLevel
                    ) + "Not find field <$columnName> from the Object File in the table <${oneConfigClass.tableName}>."
                )
                exitProcess(-1)
            }
        }

        logger.debug(
            CommonFunctions().createObjectIdForLogMsg(
                oneConfigClass.code,
                oneConfigClass.keyFieldOut,
                objFieldsList,
                nestedLevel
            ) + "All column name are identical."
        )
        queryResult.close()
    }

    // сравнение значения полей из файла со значениями колонок в таблице базы приемника.
    /*private fun checkRefFieldValue(
        objFieldsList: List<Fields>,
        oneConfigClass: ObjectCfg,
        nestedLevel: Int,
        idObjectInDB: String
    ) {

        var filterObjCond = ""
        if (oneConfigClass.filterObjects != "") {
            filterObjCond += " and ${oneConfigClass.filterObjects} "
        }

        // запрос на поиск референсного объекта. его идентификатор в базе приемнике уже известен
        val sqlQuery = "select * " +
                "from ${oneConfigClass.tableName} " +
                "where ${oneConfigClass.keyFieldIn}='$idObjectInDB' and audit_state = 'A' $filterObjCond"
        logger.trace(
            CommonFunctions().createObjectIdForLogMsg(
                oneConfigClass.code,
                oneConfigClass.keyFieldOut,
                objFieldsList,
                nestedLevel
            ) + "Query to find reference object by id in the receiver database and compare column values with the values from file. $sqlQuery."
        )
        val queryStatement = conn.prepareStatement(sqlQuery)
        val queryResult = queryStatement.executeQuery()

        // формирую список полей, которые нужно проверить
        while (queryResult.next()) {
            for (i in 1..queryResult.metaData.columnCount) {

                // название поля таблицы
                val fieldName = queryResult.metaData.getColumnName(i)
                // значение поля таблицы
                val fieldValue = queryResult.getString(i)

                // Если
                // -название колонки из таблицы не найдено среди полей, которые не нужно проверять И
                // -в поля исключения добавляю keyFieldIn (id) И
                // -значение колонки из таблицы не равно значению поля из файла,
                // то ошибка
                if (//oneConfigClass.refObjects.find { it.refField == fieldName } == null &&
                    oneConfigClass.fieldsNotExport.find { it.name == fieldName } == null &&
                    //oneConfigClass.fieldsNotExport.find { it.name == "id" } == null &&
                    fieldName != oneConfigClass.keyFieldIn &&
                    objFieldsList.find { it.fieldName == fieldName }!!.fieldValue != fieldValue) {
                    logger.error(
                        CommonFunctions().createObjectIdForLogMsg(
                            oneConfigClass.code,
                            oneConfigClass.keyFieldOut,
                            objFieldsList,
                            nestedLevel
                        ) + "The reference object does not match with the object from receiver database." +
                                "The database object ID <$idObjectInDB>. The difference in the column <$fieldName>."
                    )
                    exitProcess(-1)
                }
            }
        }
        logger.debug(
            CommonFunctions().createObjectIdForLogMsg(
                oneConfigClass.code,
                oneConfigClass.keyFieldOut,
                objFieldsList,
                nestedLevel
            ) + "The reference object was found by id in the receiver database and it column values are identical with the values from file."
        )
        queryResult.close()
    }*/

    // Сравнение значения полей из файла со значениями колонок в таблице базы приемника для референсного объекта.
    private fun checkRefFieldValue(
        objFieldsList: List<Fields>,
        oneConfigClass: ObjectCfg,
        nestedLevel: Int,
        idObjectInDB: String
    ) {

        val differentFieldName = checkFieldValue(objFieldsList, oneConfigClass, idObjectInDB)

        if (differentFieldName != "") {
            logger.error(
                CommonFunctions().createObjectIdForLogMsg(
                    oneConfigClass.code,
                    oneConfigClass.keyFieldOut,
                    objFieldsList,
                    nestedLevel
                ) + "The reference object does not match with the object from receiver database." +
                        "The database object ID <$idObjectInDB>. The difference in the column <$differentFieldName>."
            )
            exitProcess(-1)
        } else {
            logger.debug(
                CommonFunctions().createObjectIdForLogMsg(
                    oneConfigClass.code,
                    oneConfigClass.keyFieldOut,
                    objFieldsList,
                    nestedLevel
                ) + "The reference object was found by id in the receiver database and it column values are identical with the values from file."
            )
        }
    }

    // Сравнение значения полей из файла со значениями колонок в таблице базы приемника.
    // Если расхождение найдено, то вернет наименования поля с расхождением, иначе вернет пустую строку
    private fun checkFieldValue(
        objFieldsList: List<Fields>,
        oneConfigClass: ObjectCfg,
        idObjectInDB: String
    ): String {

        var resultFieldName = ""

        var filterObjCond = ""
        if (oneConfigClass.filterObjects != "") {
            filterObjCond += " and ${oneConfigClass.filterObjects} "
        }

        // запрос на поиск референсного объекта. его идентификатор в базе приемнике уже известен
        val sqlQuery = "select * " +
                "from ${oneConfigClass.tableName} " +
                "where ${oneConfigClass.keyFieldIn}='$idObjectInDB' and audit_state = 'A' $filterObjCond"
        logger.trace(
            CommonFunctions().createObjectIdForLogMsg(
                oneConfigClass.code,
                oneConfigClass.keyFieldOut,
                objFieldsList,
                -1
            ) + "Query to find reference object by id in the receiver database and compare column values with the values from file. $sqlQuery."
        )
        val queryStatement = conn.prepareStatement(sqlQuery)
        val queryResult = queryStatement.executeQuery()

        // формирую список полей, которые нужно проверить
        while (queryResult.next()) {
            for (i in 1..queryResult.metaData.columnCount) {

                // название поля таблицы
                val fieldName = queryResult.metaData.getColumnName(i)
                // значение поля таблицы
                val fieldValue = queryResult.getString(i)

                // Если
                // -название колонки из таблицы не найдено среди полей, которые не нужно проверять И
                // -в поля исключения добавляю keyFieldIn (id) И
                // -значение колонки из таблицы не равно значению поля из файла,
                // то ошибка
                if (oneConfigClass.fieldsNotExport.find { it.name == fieldName } == null &&
                    fieldName != oneConfigClass.keyFieldIn &&
                    objFieldsList.find { it.fieldName == fieldName }!!.fieldValue != fieldValue) {

                    resultFieldName = fieldName
                    break
                }
            }

            if (resultFieldName != "") {
                break
            }

        }
        queryResult.close()

        return resultFieldName
    }

    // поиск референсного объекта среди главных объектов.
    //   возвращает -1 если референсный объект не найден среди главных,
    //   либо индекс главного объекта в списке allCheckObject
    public fun findRefObjAmongMainObj(
        oneRefObject: RefObject,
        oneConfClassRefObj: ObjectCfg,
        allCheckObject: List<DataDB>
    ): Int {

        var indexInCheckObject: Int = -1
        val fieldName = oneConfClassRefObj.keyFieldIn
        val fieldValue = oneRefObject.row.fields.find { it.fieldName == fieldName }!!.fieldValue

        // цикл по главным объектам того же класса, что и референсный
        for (oneMainObject in allCheckObject.filter { it.code == oneConfClassRefObj.code }) {
            if (Fields(fieldName, fieldValue) in oneMainObject.row.fields) {
                indexInCheckObject = allCheckObject.indexOfFirst { it == oneMainObject }
                break
            }
        }

        return indexInCheckObject
    }

    // Проверка на цикличность ссылок референсных объектов.
    // Пример цикличности: Возможен вариант при котором объекты ссылаются друг на друга.
    // В этом случае объект class1.refFieldId1 ссылается на class2.refFieldId1,
    // который в свою очередь ссылается на class1.refFieldId1.
    private fun checkRingReference(
        chainCheckObject: MutableList<DataDB>,
        oneConfCheckObj: ObjectCfg,
        jsonConfigFile: RootCfg,
        allCheckObject: List<DataDB>
    ) {

        val oneCheckObject = chainCheckObject.last()

        // если у главного объекта есть референсы, которые тоже являются главным объектом,
        // то сначала нужно найти эти референсы
        for (oneRefObject in oneCheckObject.row.refObject) {

            // поиск описания класса референсного объекта в файле конфигурации
            val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneRefObject.code }!!

            // поиск референсного объекта среди главных объектов.
            // если indexInAllCheckObject > -1, то референсный объект найден среди главных
            val indexInAllCheckObject = findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allCheckObject)

            // нашли референсный объект среди главных объектов. Теперь проверка референсов найденного главного объекта
            if (indexInAllCheckObject > -1) {

                // если в цепочке референсов встретили объект проверяемый главный объект, то ошибка
                if (/*chainCheckObject.contains(allCheckObject[indexInAllCheckObject])*/ chainCheckObject.first() == allCheckObject[indexInAllCheckObject] &&
                    oneRefObject.typeRef.lowercase() != "inparent" && oneRefObject.typeRef.lowercase() != "inchild"
                ) {
                    val mainClass = jsonConfigFile.objects.find { it.code == chainCheckObject.first().code }!!
                    logger.error(
                        CommonFunctions().createObjectIdForLogMsg(
                            oneRefObject.code,
                            oneConfCheckObj.keyFieldIn,
                            oneRefObject.row.fields,
                            -1
                        ) + "The object is main and it occurs as reference object for other main object <${oneConfCheckObj.code}>, " +
                                "Object.${oneConfCheckObj.keyFieldIn}=${oneCheckObject.row.fields.find { it.fieldName == mainClass.keyFieldIn }!!.fieldValue}. " +
                                "The first object in the chain: Class Object <${mainClass.code}>, " +
                                "Object.${mainClass.keyFieldIn}=${chainCheckObject.first().row.fields.find { it.fieldName == mainClass.keyFieldIn }!!.fieldValue}"
                    )
                    exitProcess(-1)
                } else if (/*chainCheckObject.contains(allCheckObject[indexInAllCheckObject])*/ chainCheckObject.first() == allCheckObject[indexInAllCheckObject] &&
                    (oneRefObject.typeRef.lowercase() == "inparent" || oneRefObject.typeRef.lowercase() == "inchild")
                ) {
                    // референсы подобного типа пропускаю, т.к. они заведомо закольцованы и при загрузке обрабатываются отдельным образом
                    continue
                }

                if (!chainCheckObject.contains(allCheckObject[indexInAllCheckObject])) {
                    chainCheckObject.add(allCheckObject[indexInAllCheckObject])

                    // рекурсивный поиск референсных объектов, которые есть среди главных объектов
                    checkRingReference(chainCheckObject, oneConfClassRefObj, jsonConfigFile, allCheckObject)
                }
            }
        }
    }

    // сравнение ссылок типа refTables объекта в бд приемнике и в файле. сравнение по кол-ву и по коду(keyFieldOut),
    //   т.е. объект из бд и из файла должен ссылаться на одни и те же refTables
    public fun compareObjectRefTables(
        idObjectInDB: String,
        oneObjectClass: ObjectCfg,
        listRefObject: List<RefObject>,
        listFieldObject: List<Fields>,
        //oneCheckObject: DataDB,
        jsonConfigFile: RootCfg
    ): Boolean {

        // возвращаемое процедурой значение
        var returnValue = true

        for (oneRefTableDescription in oneObjectClass.refTables) {

            // класс ссылки refTables
            val oneRefTablesClass = jsonConfigFile.objects.find { it.code == oneRefTableDescription.codeRef }

            if (oneRefTablesClass == null) {
                logger.error(
                    CommonFunctions().createObjectIdForLogMsg(oneRefTableDescription.codeRef, "", null, 0) +
                            "The class was not found in the configuration file."
                )
                exitProcess(-1)
            }

            // строка кодов объектов refTables из бд приемника
            val listRefTablesCodeInDB = mutableListOf<String>()
            // кол-во ссылок refTables в бд приемнике
            var countRefTablesObjInDB = 0

            // строка кодов объектов refTables из файла
            val listRefTablesCodeInFile = mutableListOf<String>()
            // кол-во ссылок refTables в файле
            val countRefTablesObjInFile: Int =
                listRefObject.filter { it.code == oneRefTablesClass.code && it.typeRef == "refTables" }.size

            for (item in listRefObject.filter { it.code == oneRefTablesClass.code && it.typeRef == "refTables" }) {
                listRefTablesCodeInFile +=
                    item.row.fields.find { it.fieldName == oneRefTablesClass.keyFieldOut }!!.fieldValue!!
            }

            var filterObjCond = ""
            if (oneRefTablesClass.filterObjects != "") {
                filterObjCond += " and ${oneRefTablesClass.filterObjects} "
            }

            val sqlQuery = "select distinct ref_tbl.${oneRefTablesClass.keyFieldOut}, count(1) over() " +
                    "from ${oneRefTableDescription.table} link_tbl " +
                    "join ${oneObjectClass.tableName} rel_tbl on rel_tbl.${oneObjectClass.keyFieldIn} = link_tbl.${oneRefTableDescription.refField} " +
                    "join ${oneRefTablesClass.tableName} ref_tbl on ref_tbl.${oneRefTablesClass.keyFieldIn} = link_tbl.${oneRefTableDescription.refFieldTo} " +
                    "where ref_tbl.audit_state = 'A' and rel_tbl.${oneObjectClass.keyFieldIn} = $idObjectInDB $filterObjCond"
            logger.trace(
                CommonFunctions().createObjectIdForLogMsg(
                    oneObjectClass.code,
                    oneObjectClass.keyFieldIn,
                    idObjectInDB
                    //listFieldObject,
                    //-1
                ) + "The query to match RefTables links for class <${oneRefTablesClass.code}>. $sqlQuery"
            )
            val connCmpObjectRefTables = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
            val queryStatement = connCmpObjectRefTables.prepareStatement(sqlQuery)
            val queryResult = queryStatement.executeQuery()
            while (queryResult.next()) {

                // кол-во ссылок refTables в бд приемнике
                countRefTablesObjInDB = queryResult.getInt(2)
                // строка кодов объектов refTables из бд приемника
                listRefTablesCodeInDB += queryResult.getString(1)

            }
            queryResult.close()
            connCmpObjectRefTables.close()

            if (countRefTablesObjInDB != countRefTablesObjInFile || listRefTablesCodeInDB.sorted() != listRefTablesCodeInFile.sorted()) {
                logger.debug(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneObjectClass.code,
                        oneObjectClass.keyFieldIn,
                        idObjectInDB
                        //listFieldObject,
                        //-1
                    ) + "RefTables links did not match for class <${oneRefTablesClass.code}>. "
                )
                returnValue = false
                break
            } else {
                logger.debug(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneObjectClass.code,
                        oneObjectClass.keyFieldIn,
                        idObjectInDB
                        //listFieldObject,
                        //-1
                    ) + "RefTables links match for class <${oneRefTablesClass.code}>. "
                )
            }
        }

        return returnValue
    }

    // получаю идентификатор шкалы
    private fun getNewScaleId(
        oneLoadObject: DataDB,
        oneConfClassObj: ObjectCfg
        //jsonConfigFile: RootCfg

    ): String {

        var idScaleInDB = ""

        // если закачиваемого тарифа нет в БД приемнике, то проверяю есть ли шкала в тарифе в файле данных и если есть, то генерю новый ид шкалы
        oneLoadObject.row.refObject.find { it.typeRef.lowercase() == "inscale" }?.let { refObject ->
            jsonConfigFile.objects.find { it.code == refObject.code }?.let { oneCfgClass ->
                idScaleInDB = loadObject.nextSequenceValue(oneCfgClass.sequence)
            }
        }

        return idScaleInDB
    }

    // установка значения идентификатора шкалы в файле
    private fun setNewIdScale(
        oneLoadObject: DataDB,
        idScaleInDB: String
    ) {

        // установка нового значения шкалы в тарифе
        var indexOfScaleField =
            oneLoadObject.row.fields.indexOfFirst {
                it.fieldName.lowercase() == scalable.getScaleIdFieldName(
                    oneLoadObject
                )
            }
        //oneLoadObject.row.fields.indexOfFirst { it.fieldName.lowercase() == CommonConstants().SCALE_ID_FIELD_NAME }
        if (indexOfScaleField > -1 && oneLoadObject.row.fields[indexOfScaleField].fieldValue != idScaleInDB) {
            oneLoadObject.row.fields[indexOfScaleField].fieldValue = idScaleInDB
        }

        // установка нового значения шкалы в scalableAmount и scaleComponent
        //for (oneLinkObject in oneLoadObject.row.linkObjects.filter { it.code.lowercase() == CommonConstants().TARIFF_VALUE_CLASS_NAME || it.code.lowercase() == CommonConstants().NUMBER_TARIFF_VALUE_CLASS_NAME }) {
        //    for (oneScaleObject in oneLinkObject.row.scaleObjects.filter { it.code.lowercase() == CommonConstants().SCALE_COMPONENT_CLASS_NAME || it.code.lowercase() == CommonConstants().SCALE_AMOUNT_CLASS_NAME }) {
        for (oneLinkObject in oneLoadObject.row.linkObjects) {
            for (oneScaleObject in oneLinkObject.row.scaleObjects) {
                indexOfScaleField =
                    oneScaleObject.row.fields.indexOfFirst {
                        it.fieldName.lowercase() == scalable.getScaleIdFieldName(
                            oneLoadObject
                        )
                    }
                //oneScaleObject.row.fields.indexOfFirst { it.fieldName.lowercase() == CommonConstants().SCALE_ID_FIELD_NAME }
                if (indexOfScaleField > -1 && oneScaleObject.row.fields[indexOfScaleField].fieldValue != idScaleInDB) {
                    oneScaleObject.row.fields[indexOfScaleField].fieldValue = idScaleInDB
                }
            }
        }
    }

    // формирование условий запроса для референсов linkObjects
    public fun createLinkObjectsQuery(
        oneLoadObject: DataDB,
        oneConfClassObj: ObjectCfg,
        jsonConfigFile: RootCfg,
        idObjectInDB: String
    ): Array<String> {

        var sqlQueryLinkObjDeclare = ""
        var sqlQueryLinkObjInit = ""
        var sqlQueryLinkObject = ""
        var cntVar = 1 // счетчик для наименования переменных

        // все ссылки linkObjects из конфигурации
        for (oneLinkObjDescription in oneConfClassObj.linkObjects.filter { it.keyType.lowercase() == "ingroup" }) {

            // класс ссылки
            val oneLinkObjClass = jsonConfigFile.objects.find { it.code == oneLinkObjDescription.codeRef }!!

            // НЕЛЬЗЯ УДАЛЯТЬ ЗАПИСИ ПРОСТАВЛЯЯ 'R'. НЕ УДАЛЯТЬ
            //////////////////////////////////////////////////////////////////////////////////////////////////
            if (false) {
                // ссылочное поле
                val refField = oneLinkObjDescription.refField

                // удаляю записи из таблицы объекта linkObjects
                sqlQueryLinkObject += "\nupdate ${oneLinkObjClass.tableName} set audit_state = 'R' where $refField='$idObjectInDB'; \n"

                // каждую удаленную запись добавляю в таблицу аудита (транзакция еще не закомичина)
                val sqlQuery =
                    "select id from ${oneLinkObjClass.tableName} where audit_state='A' and $refField='$idObjectInDB'"
                val queryStatement = conn.prepareStatement(sqlQuery)
                val queryResult = queryStatement.executeQuery()
                while (queryResult.next()) {
                    // идентификатор очередной удаленной записи
                    val idValue = queryResult.getString(1)

                    // название переменной для значения сиквенса
                    val nextValueRevLinkName = "nextValueRevLink$cntVar"
                    // увеличение счетчика переменных
                    cntVar++
                    // объявление переменных для значений сиквенса в psql
                    sqlQueryLinkObjDeclare += "declare $nextValueRevLinkName integer; \n"
                    // инициализация переменных для сиквенса в psql
                    sqlQueryLinkObjInit += "$nextValueRevLinkName=nextval('${CommonConstants().commonSequence}'); \n"

                    // insert в таблицу аудита
                    sqlQueryLinkObject +=
                            //createAudInsQuery(oneLinkClass, idValue, nextValueRevLinkName)
                        loadObject.createMainInsUpdQuery(
                            null,
                            oneLinkObjClass,
                            idValue,
                            nextValueRevLinkName,
                            "insertAudSelectValue"
                        )

                }
                queryResult.close()
            }
            //////////////////////////////////////////////////////////////////////////////////////////////////

            // проверка всех linkObjects из приемника с записями в файле загрузки
            //   сравниваю значение всех(кроме id) полей объекта linkObjects из базы и файла.
            //   затем сравниваю кол-во ссылок refTables объекта в базе и файле. проверяю что объект ссылается на одинаковые объекты refTables
            // ищу все linkObjects необходимого класса в бд приемнике

            // список linkObjects из файла, для которых в приемнике нашелся точно такой же
            val listLinkObjNotToLoad = mutableListOf<String>()

            if (idObjectInDB != "-") {
                var filterObjCond = ""
                if (oneLinkObjClass.filterObjects != "") {
                    filterObjCond += " and ${oneLinkObjClass.filterObjects} "
                }
                val sqlQuery = "select * " +
                        "from ${oneLinkObjClass.tableName} " +
                        "where audit_state = 'A' and ${oneLinkObjDescription.refField} = $idObjectInDB $filterObjCond "
                //"and (valid_to is null or valid_to >= CURRENT_DATE)"

                logger.trace(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneLinkObjClass.code,
                        "",
                        null,
                        -1
                    ) + "The query to find objects for linkObject class <${oneConfClassObj.code}.${oneConfClassObj.keyFieldIn}=$idObjectInDB>. $sqlQuery"
                )

                val queryStatement = conn.prepareStatement(sqlQuery)
                val queryResult = queryStatement.executeQuery()
                // цикл по всем записям linkObjects из бд приемника
                while (queryResult.next()) {
                    // формирую список полей и значений для одной записи, которую нужно проверить
                    val linkObjectFieldsFromDB = mutableListOf<Fields>()
                    var linkObjectFromDBId = "-1"
                    var isLinkObjectFindInFile = false
                    for (i in 1..queryResult.metaData.columnCount) {
                        // название поля таблицы
                        val fieldName = queryResult.metaData.getColumnName(i)
                        // значение поля таблицы
                        val fieldValue = queryResult.getString(i)

                        if (oneLinkObjClass.fieldsNotExport.find { it.name == fieldName } == null) {
                            linkObjectFieldsFromDB.add(Fields(fieldName, fieldValue))
                            if (fieldName == oneLinkObjClass.keyFieldIn) {
                                linkObjectFromDBId = fieldValue
                            }
                        }
                    }

                    // сравниваю объект из бд приемника со всеми объектами linkObjects того же класса из загружаемого главного объекта oneLoadObject
                    for (oneLinkObject in oneLoadObject.row.linkObjects.filter { it.code == oneLinkObjClass.code }) {

                        // ид linkObject из файла
                        val linkObjectFromFileId =
                            oneLinkObject.row.fields.find { it.fieldName == oneLinkObjClass.keyFieldIn }!!.fieldValue!!

                        // сравниваю значение полей
                        var isLinkObjectFieldsEqual = true
                        for ((fieldName, fieldValue) in linkObjectFieldsFromDB) {
                            if (fieldName != oneLinkObjClass.keyFieldIn && oneLinkObject.row.fields.find { it.fieldName == fieldName }!!.fieldValue != fieldValue &&
                                oneLinkObjClass.scale?.find { it.refField == fieldName } == null
                            ) {
                                logger.debug(
                                    CommonFunctions().createObjectIdForLogMsg(
                                        oneLinkObjClass.code,
                                        oneLinkObjClass.keyFieldIn,
                                        linkObjectFieldsFromDB,
                                        -1
                                    ) + "The linkObject from receiver database did not match with the linkObject from file." +
                                            //"The database object ID <$linkObjectFromDBFieldId>, " +
                                            "The file linkObject ID <$linkObjectFromFileId>. " +
                                            "The difference in the column <$fieldName>."
                                )
                                isLinkObjectFieldsEqual = false
                                break
                            }
                        }

                        // сравниваю референсы refTables
                        if (isLinkObjectFieldsEqual) {

                            logger.debug(
                                CommonFunctions().createObjectIdForLogMsg(
                                    oneLinkObjClass.code,
                                    oneLinkObjClass.keyFieldIn,
                                    linkObjectFieldsFromDB,
                                    -1
                                ) + "The linkObject from receiver database has been found in the file by identical field values. " +
                                        "The file linkObject ID <$linkObjectFromFileId>."
                            )

                            // Важно. Заменяю значение поля tariff_value.scale_component_id из файла на найденное значение из БД
                            val valueOfScaleComponentIdFieldFromDB =
                                //linkObjectFieldsFromDB.find { it.fieldName == CommonConstants().SCALE_COMPONENT_ID_FIELD_NAME }?.fieldValue
                                linkObjectFieldsFromDB.find { it.fieldName == scalable.scaleComponentFieldName }?.fieldValue
                            val indexOfScaleComponentIdField =
                                //oneLinkObject.row.fields.indexOfFirst { it.fieldName == CommonConstants().SCALE_COMPONENT_ID_FIELD_NAME }
                                oneLinkObject.row.fields.indexOfFirst { it.fieldName == scalable.scaleComponentFieldName }
                            if (indexOfScaleComponentIdField > -1) {
                                oneLinkObject.row.fields[indexOfScaleComponentIdField].fieldValue =
                                    valueOfScaleComponentIdFieldFromDB
                            }

                            //if (CheckObject().compareObjectRefTables(
                            if (compareObjectRefTables(
                                    linkObjectFromDBId,
                                    oneLinkObjClass,
                                    oneLinkObject.row.refObject,
                                    oneLinkObject.row.fields,
                                    jsonConfigFile
                                )
                            ) {

                                // проверка шкал
                                // объекты scaleObjects из базы совпадают/не совпадают с такими же объектами из tariffValue
                                var isIdenticalScale = true

                                // проверяю есть ли шкала у тарифа в БД приемнике. если шкалы нет, значит объекты scaleObjects нужно добавить
                                val loadObjectClass = jsonConfigFile.objects.find { it.code == oneLoadObject.code }!!
                                val idScaleInDB = loadObject.findScaleIdInDB(loadObjectClass, idObjectInDB)

                                // поиск в БД приемнике scaleObjects и их сравнение с аналогичными объектами из файла
                                if (idScaleInDB != "") {
                                    val linkObjectClass =
                                        jsonConfigFile.objects.find { it.code == oneLinkObject.code }!!
                                    //isIdenticalScale = ReaderDB().readScaleObject(
                                    isIdenticalScale = readerDB.readScaleObject(
                                        linkObjectClass,
                                        jsonConfigFile,
                                        oneLinkObject.row.fields,
                                        "Load",
                                        oneLinkObject.row.scaleObjects
                                    )
                                }

                                if (isIdenticalScale) {
                                    listLinkObjNotToLoad.add(linkObjectFromFileId)
                                    isLinkObjectFindInFile = true
                                    logger.debug(
                                        CommonFunctions().createObjectIdForLogMsg(
                                            oneLinkObjClass.code,
                                            oneLinkObjClass.keyFieldIn,
                                            oneLinkObject.row.fields,
                                            -1
                                        ) + "The linkObject from file was found in database. The database linkObject ID <$linkObjectFromDBId>. It will not be added to the database."
                                    )
                                    //break
                                }
                            }
                        }
                    }

                    // объект linkObjects из базы не найден среди загружаемых, поэтому проставим ему дату "По..." вчерашним числом
                    if (!isLinkObjectFindInFile) {

                        logger.debug(
                            CommonFunctions().createObjectIdForLogMsg(
                                oneLinkObjClass.code,
                                oneLinkObjClass.keyFieldIn,
                                linkObjectFieldsFromDB,
                                -1
                            ) + "The linkObject from receiver database was not found in the file. Its <valid_to> date will be updated."
                        )

                        sqlQueryLinkObject += "\nupdate ${oneLinkObjClass.tableName} set valid_to=CURRENT_DATE-1 where ${oneLinkObjClass.keyFieldIn}=$linkObjectFromDBId; \n"

                        // название переменной для значения сиквенса
                        val nextValueRevLinkName = "nextValueRevLink$cntVar"
                        // увеличение счетчика переменных
                        cntVar++
                        // объявление переменных для значений сиквенса в psql
                        sqlQueryLinkObjDeclare += "declare $nextValueRevLinkName integer; \n"
                        // инициализация переменных для сиквенса в psql
                        sqlQueryLinkObjInit += "$nextValueRevLinkName=nextval('${CommonConstants().commonSequence}'); \n"
                        // insert в таблицу аудита новой записи объекта linkObject
                        sqlQueryLinkObject +=
                            loadObject.createMainInsUpdQuery(
                                null,
                                oneLinkObjClass,
                                linkObjectFromDBId,
                                nextValueRevLinkName,
                                "insertAudSelectValue"
                            )
                    }
                }
                queryResult.close()
            }

            // все объекты linkObjects того же класса из загружаемого главного объекта oneLoadObject
            for (oneLinkObject in oneLoadObject.row.linkObjects.filter { it.code == oneLinkObjClass.code }) {

                // пропускаю linkObject, для которых нашлось полное соответствие в приемнике. такой linkObject грузить в приемник не нужно
                if (listLinkObjNotToLoad.contains(oneLinkObject.row.fields.find { it.fieldName == oneLinkObjClass.keyFieldIn }!!.fieldValue!!)) {
                    continue
                }

                logger.debug(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneLinkObjClass.code,
                        oneLinkObjClass.keyFieldIn,
                        oneLinkObject.row.fields,
                        -1
                    ) + "The linkObject from file will be inserted in receiver database."
                )

                // запрос для вставки scaleObject. должен быть перед формированием запроса на вставку linkObject (sqlQueryLinkObject),
                //   т.к. внутри createScaleObjectsQuery генерится новый scale_component_id и сохраняется в linkObject из файла
                //if (oneLinkObject.code.lowercase() == CommonConstants().TARIFF_VALUE_CLASS_NAME || oneLinkObject.code.lowercase() == CommonConstants().NUMBER_TARIFF_VALUE_CLASS_NAME) {
                if (scalable.isClassHaveScaleComponent(oneLinkObject)) {
                    val listQueryCondition = createScaleObjectsQuery(oneLinkObject, jsonConfigFile, cntVar)
                    sqlQueryLinkObjDeclare += listQueryCondition[0]
                    sqlQueryLinkObjInit += listQueryCondition[1]
                    sqlQueryLinkObject += listQueryCondition[2]
                    cntVar = listQueryCondition[3].toInt()
                }

                val idLinkObjectInDB = loadObject.nextSequenceValue(oneLinkObjClass.sequence)

                // insert в таблицу объекта linkObject
                sqlQueryLinkObject += "\n" +
                        loadObject.createMainInsUpdQuery(
                            oneLinkObject,
                            oneLinkObjClass,
                            idLinkObjectInDB,
                            "",
                            "insert"
                        )

                // название переменной для значения сиквенса
                val nextValueRevLinkName = "nextValueRevLink$cntVar"
                // увеличение счетчика переменных
                cntVar++
                // объявление переменных для значений сиквенса в psql
                sqlQueryLinkObjDeclare += "declare $nextValueRevLinkName integer; \n"
                // инициализация переменных для сиквенса в psql
                sqlQueryLinkObjInit += "$nextValueRevLinkName=nextval('${CommonConstants().commonSequence}'); \n"

                // insert в таблицу аудита новой записи объекта linkObject
                sqlQueryLinkObject +=
                    loadObject.createMainInsUpdQuery(
                        oneLinkObject,
                        oneLinkObjClass,
                        idLinkObjectInDB,
                        nextValueRevLinkName,
                        "insertAudValues"
                    )

                // insert в таблицу связей refObject для объекта linkObject
                sqlQueryLinkObject += "\n" +
                        loadObject.createRefTablesQuery(
                            oneLinkObject,
                            oneLinkObjClass,
                            null,
                            jsonConfigFile,
                            idLinkObjectInDB,
                            "refTables"
                        )
            }
        }

        return arrayOf(sqlQueryLinkObjDeclare, sqlQueryLinkObjInit, sqlQueryLinkObject)
    }

    public fun createScaleObjectsQuery(
        oneLinkObject: DataDB,
        jsonConfigFile: RootCfg,
        cntVariable: Int
    ): Array<String> {

        // нужно добавлять в БД приемник новые объекты шкалы

        var cntVar = cntVariable
        var sqlQueryScale = ""
        var sqlQueryScaleObjDeclare = ""
        var sqlQueryScaleObjInit = ""

        var idScaleComponentObjectInDB = ""
        var idScalableAmountObjectInDB = ""
        var indexOfField = -1
        var nextValueRevScaleName = ""
        var idScalableAmountObjectInFile = ""

        //oneLinkObject.row.scaleObjects.find { it.code.lowercase() == CommonConstants().SCALE_COMPONENT_CLASS_NAME }
        oneLinkObject.row.scaleObjects.find { it.code.lowercase() == scalable.scaleComponentClassName }
            ?.let { scaleComponentObject ->
                jsonConfigFile.objects.find { it.code == scaleComponentObject.code }?.let { scaleComponentClass ->
                    idScaleComponentObjectInDB = loadObject.nextSequenceValue(scaleComponentClass.sequence)

                    // перед формированием запроса для объекта linkObject подставляю новое значение поля scale_component_id
                    indexOfField =
                            //oneLinkObject.row.fields.indexOfFirst { it.fieldName.lowercase() == CommonConstants().SCALE_COMPONENT_ID_FIELD_NAME }
                        oneLinkObject.row.fields.indexOfFirst { it.fieldName.lowercase() == scalable.scaleComponentFieldName }
                    oneLinkObject.row.fields[indexOfField].fieldValue = idScaleComponentObjectInDB

                    // insert в таблицу объекта scaleComponent
                    sqlQueryScale += "\n" +
                            loadObject.createMainInsUpdQuery(
                                scaleComponentObject,
                                scaleComponentClass,
                                idScaleComponentObjectInDB,
                                "",
                                "insert"
                            )

                    // название переменной для значения сиквенса
                    nextValueRevScaleName = "nextValueRevScale$cntVar"
                    cntVar++

                    // объявление переменных для значений сиквенса в psql
                    sqlQueryScaleObjDeclare += "declare $nextValueRevScaleName integer; \n"
                    // инициализация переменных для сиквенса в psql
                    sqlQueryScaleObjInit += "$nextValueRevScaleName=nextval('${CommonConstants().commonSequence}'); \n"

                    // insert в таблицу аудита новой записи
                    sqlQueryScale +=
                        loadObject.createMainInsUpdQuery(
                            scaleComponentObject,
                            scaleComponentClass,
                            idScaleComponentObjectInDB,
                            nextValueRevScaleName,
                            "insertAudValues"
                        )

                    // работа с объектами scalableAmount
                    // их нужно добавлять только если в базе(файле) не нашёлся такой же объект, в т.ч. совпадающий по полю scale_id
                    //for (scalableAmountObject in oneLinkObject.row.scaleObjects.filter { it.code.lowercase() == CommonConstants().SCALE_AMOUNT_CLASS_NAME }) {
                    for (scalableAmountObject in oneLinkObject.row.scaleObjects.filter { it.code.lowercase() == scalable.scalableAmountClassName }) {
                        jsonConfigFile.objects.find { it.code == scalableAmountObject.code }
                            ?.let { scalableAmountClass ->

                                idScalableAmountObjectInFile =
                                    scalableAmountObject.row.fields.find { it.fieldName == scalableAmountClass.keyFieldIn }!!.fieldValue!!

                                idScalableAmountObjectInDB = ""
                                // проверка есть ли объект scalableAmount в БД приемнике по полному соответствию (включая поле scale_id)
                                val sqlQuery =
                                    loadObject.createMainInsUpdQuery(
                                        scalableAmountObject,
                                        scalableAmountClass,
                                        "",
                                        "",
                                        "selectScalableAmount"
                                    )
                                val connSelect = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
                                val queryStatement = connSelect.prepareStatement(sqlQuery)
                                val queryResult = queryStatement.executeQuery()
                                while (queryResult.next()) {
                                    idScalableAmountObjectInDB = queryResult.getString(1)
                                }
                                connSelect.close()

                                // ищу объект scalableAmount не в БД, а среди добавленных для тарифа. Это актуально для случая создания новой шкалы/нового тарифа
                                val newScalableAmountObject = mutableListOf<Fields>()
                                for ((fieldName, fieldValue) in scalableAmountObject.row.fields) {
                                    if (scalableAmountClass.fieldsNotExport.find { it.name == fieldName } == null &&
                                        fieldName != scalableAmountClass.keyFieldIn) {
                                        newScalableAmountObject.add(Fields(fieldName, fieldValue))
                                    }
                                }
                                listNewScalableAmountObject.find { it.fields == newScalableAmountObject }.let { newId ->
                                    if (newId != null) {
                                        idScalableAmountObjectInDB = newId.newId
                                    }
                                }

                                // если объекта нет, то insert в таблицу объекта scalableAmount если не найдено полного соответствия
                                if (idScalableAmountObjectInDB == "") {
                                    idScalableAmountObjectInDB =
                                        loadObject.nextSequenceValue(scalableAmountClass.sequence)
                                    sqlQueryScale += "\n" +
                                            loadObject.createMainInsUpdQuery(
                                                scalableAmountObject,
                                                scalableAmountClass,
                                                idScalableAmountObjectInDB,
                                                "",
                                                "insert"
                                            )

                                    nextValueRevScaleName = "nextValueRevScale$cntVar"
                                    cntVar++

                                    // объявление переменных для значений сиквенса в psql
                                    sqlQueryScaleObjDeclare += "declare $nextValueRevScaleName integer; \n"
                                    // инициализация переменных для сиквенса в psql
                                    sqlQueryScaleObjInit += "$nextValueRevScaleName=nextval('${CommonConstants().commonSequence}'); \n"

                                    // insert в таблицу аудита новой записи объекта linkObject
                                    sqlQueryScale +=
                                        loadObject.createMainInsUpdQuery(
                                            scalableAmountObject,
                                            scalableAmountClass,
                                            idScalableAmountObjectInDB,
                                            nextValueRevScaleName,
                                            "insertAudValues"
                                        )

                                    // формирую список новых объектов scalableAmount с новым id в рамках тарифа
                                    /*if (!listNewScalableAmountObject.contains(
                                            ScaleAmountInDB(newScalableAmountObject, idScalableAmountObjectInDB)
                                        )
                                    ) {*/
                                    listNewScalableAmountObject.add(
                                        ScaleAmountInDB(newScalableAmountObject, idScalableAmountObjectInDB)
                                    )
                                    //}
                                }

                                // работа с объектами scaleComponentValue
                                //for (scaleComponentValueObject in oneLinkObject.row.scaleObjects.filter { it.code.lowercase() == CommonConstants().SCALE_COMPONENT_VALUE_CLASS_NAME }
                                //.filter { it.row.fields.find { fields -> fields.fieldName.lowercase() == "scalable_amount_id" && fields.fieldValue == idScalableAmountObjectInFile } != null }
                                for (scaleComponentValueObject in oneLinkObject.row.scaleObjects.filter { it.code.lowercase() == scalable.scaleComponentValueClassName }
                                    .filter { it.row.fields.find { fields -> fields.fieldName.lowercase() == scalable.scalableAmountFieldName && fields.fieldValue == idScalableAmountObjectInFile } != null }
                                ) {
                                    jsonConfigFile.objects.find { it.code == scaleComponentValueObject.code }
                                        ?.let { scaleComponentValueClass ->
                                            val idScaleComponentValueObjectInDB =
                                                loadObject.nextSequenceValue(scaleComponentValueClass.sequence)

                                            // перед формированием запроса для объекта scaleComponentValue подставляю значение полей component_id и scalable_amount_id
                                            indexOfField =
                                                scaleComponentValueObject.row.fields.indexOfFirst { it.fieldName.lowercase() == scalable.scaleComponentValueFieldName }
                                            //scaleComponentValueObject.row.fields.indexOfFirst { it.fieldName.lowercase() == "component_id" }
                                            scaleComponentValueObject.row.fields[indexOfField].fieldValue =
                                                idScaleComponentObjectInDB

                                            indexOfField =
                                                scaleComponentValueObject.row.fields.indexOfFirst { it.fieldName.lowercase() == scalable.scalableAmountFieldName }
                                            //scaleComponentValueObject.row.fields.indexOfFirst { it.fieldName.lowercase() == "scalable_amount_id" }
                                            scaleComponentValueObject.row.fields[indexOfField].fieldValue =
                                                idScalableAmountObjectInDB

                                            // insert в таблицу объекта trf_scale_component_value
                                            sqlQueryScale += "\n" +
                                                    loadObject.createMainInsUpdQuery(
                                                        scaleComponentValueObject,
                                                        scaleComponentValueClass,
                                                        idScaleComponentValueObjectInDB,
                                                        "",
                                                        "insert"
                                                    )

                                            nextValueRevScaleName = "nextValueRevScale$cntVar"
                                            cntVar++

                                            // объявление переменных для значений сиквенса в psql
                                            sqlQueryScaleObjDeclare += "declare $nextValueRevScaleName integer; \n"
                                            // инициализация переменных для сиквенса в psql
                                            sqlQueryScaleObjInit += "$nextValueRevScaleName=nextval('${CommonConstants().commonSequence}'); \n"

                                            // insert в таблицу аудита новой записи объекта linkObject
                                            sqlQueryScale +=
                                                loadObject.createMainInsUpdQuery(
                                                    scaleComponentValueObject,
                                                    scaleComponentValueClass,
                                                    idScaleComponentValueObjectInDB,
                                                    nextValueRevScaleName,
                                                    "insertAudValues"
                                                )
                                        }
                                }
                            }
                    }
                }
            }

        return arrayOf(sqlQueryScaleObjDeclare, sqlQueryScaleObjInit, sqlQueryScale, cntVar.toString())
    }

    // установка нового значения ссылочного поля в файле для референса типа refFields
    private fun setNewIdRefFields(
        //oneLoadObject: DataDB,
        oneLoadObjFields: List<Fields>,
        oneLoadObjRef: List<RefObject>,
        oneConfClassObj: ObjectCfg,
        jsonConfigFile: RootCfg,
        oneMainLoadObject: DataDB? // главный объект, по которому проверяются linkObject. Заполняется только если проверяется объект из linkObject
    ) {
        for (oneReferenceDescr in oneConfClassObj.refObjects) {

            var idObjectInDB = "-"
            // название референсного поля
            val refField = oneReferenceDescr.refField
            // класс референсного объекта
            val oneRefClassObj = jsonConfigFile.objects.find { it.code == oneReferenceDescr.codeRef }!!

            // референсный объект должен быть всегда один (или ни одного)
            val oneRefObject = oneLoadObjRef.find { it.code == oneReferenceDescr.codeRef && it.fieldRef == refField }

            if (oneRefObject == null && oneMainLoadObject != null && oneReferenceDescr.codeRef == oneMainLoadObject.code) {
                // Обработка ссылки объекта из списка linkObject, которая указывает на главный объект.
                // Пример ссылки: tariffCondition.tariff_id указывает на объект tariff
                // Такого референса нет в файле данных(он не выгружается), т.к. иначе было бы кольцо: ссылочный объект tariff из объекта tariffCondition указывал бы сам на себя
                // Тем не менее нужно получить ид этого tariff_id в бд приемнике
                val oneMainLoadObjClass = jsonConfigFile.objects.find { it.code == oneMainLoadObject.code }!!
                // поиск id референсного объекта в бд приемнике
                idObjectInDB = loadObject.getObjIdInDB(oneMainLoadObject.row.fields, oneMainLoadObjClass, 0, true)
            } else if (oneRefObject == null) {
                continue
            } else if (oneRefObject.typeRef.lowercase() == "inchild") {
                idObjectInDB =
                    loadObject.getObjIdInDB(oneRefObject.row.fields, oneRefClassObj, oneRefObject.nestedLevel, false)
            } else {
                // поиск id референсного объекта в бд приемнике
                idObjectInDB =
                    loadObject.getObjIdInDB(oneRefObject.row.fields, oneRefClassObj, oneRefObject.nestedLevel, true)
            }

            // для референса inchild нужно записать значение null, т.к. реальное значение еще неизвестно
            if (idObjectInDB == "-" && oneRefObject!!.typeRef.lowercase() != "inchild") {
                logger.error(
                    CommonFunctions().createObjectIdForLogMsg(
                        oneConfClassObj.code,
                        oneConfClassObj.keyFieldOut,
                        oneLoadObjFields,
                        -1
                    ) + "<The reference object <$refField.=${oneLoadObjFields.find { it.fieldName == refField }!!.fieldValue.toString()}>" +
                            " was not found in the receiver database>. "
                )
                exitProcess(-1)
                //exitFromProgram()
            }

            // установка нового значения референса
            val indexOfRefField = oneLoadObjFields.indexOfFirst { it.fieldName == refField }
            if (idObjectInDB == "-" && oneRefObject != null && oneRefObject.typeRef.lowercase() == "inchild") {
                oneLoadObjFields[indexOfRefField].fieldValue = null
            } else if (oneLoadObjFields[indexOfRefField].fieldValue != idObjectInDB && oneReferenceDescr.keyType.lowercase() != "out") {
                oneLoadObjFields[indexOfRefField].fieldValue = idObjectInDB
            }

        }
    }

    // установка нового значения ссылочного поля для референса типа fieldJson
    private fun setNewIdFieldJson(
        oneLoadObjFields: List<Fields>,
        oneLoadObjRef: List<RefObject>,
        oneConfClassObj: ObjectCfg,
        jsonConfigFile: RootCfg
    ) {

        // цикл по референсам типа refFieldsJson в классе (разные референсы могут храниться в разных полях)
        for (oneRefFieldsJson in oneConfClassObj.refFieldsJson) {
            // проверка, что такие референсы есть
            if (!oneRefFieldsJson.refObjects.isNullOrEmpty()) {
                // цикл по референсам внутри одного референса refFieldsJson (в одном поле могут храниться референсы на разные объекты)
                for (oneRefFieldJson in oneRefFieldsJson.refObjects) {

                    // класс референса
                    val oneRefFieldJsonClass = jsonConfigFile.objects.find { it.code == oneRefFieldJson.codeRef }!!

                    // название поля в строке fieldJson, значение в котором нужно изменить
                    val fieldNameInJsonStr = oneRefFieldJson.refField//.lowercase()

                    // строка fieldJson
                    val indexOfJsonField = oneLoadObjFields.indexOfFirst { it.fieldName == oneRefFieldsJson.name }
                    val jsonStr = oneLoadObjFields[indexOfJsonField].fieldValue
                        ?: continue // если null, значит по условиям схемы допустимо, что в поле fieldJson значение null

                    // зачитываю json-строку атрибутов в дерево
                    val attribute = ReadJsonFile().readJsonStrAsTree(jsonStr)
                    val attributeRecord = attribute[oneRefFieldJson.record]

                    if (attributeRecord != null && attributeRecord.size() > 0) {
                        // для каждого значения референса из строки атрибутов ищу референс среди референсов переданного объекта
                        for ((index, itemFromAttribute) in attributeRecord.withIndex()) {

                            // ид референса в бд источнике
                            var fieldValueInJsonStr: String
                            if (itemFromAttribute is ObjectNode) { // для вариантов вида {\"restrictions\":[{\"finInstitutionId\":\"100001\",\"restrictionActionType\":\"BLOCK\"},{\"finInstitutionId\":\"100002\",\"restrictionActionType\":\"BLOCK\"}]}
                                fieldValueInJsonStr = itemFromAttribute.get(fieldNameInJsonStr).asText()
                            } else if (attributeRecord is ObjectNode) { // для вариантов вида {\"restrictions\":{\"finInstitutionId\":\"100001\",\"restrictionActionType\":\"BLOCK\"}}
                                fieldValueInJsonStr = attributeRecord.get(fieldNameInJsonStr).asText()
                            } else { // для вариантов вида {"systemGroupList":[100000,100002]}; {"systemGroupList":[100000]}
                                fieldValueInJsonStr = itemFromAttribute.asText()
                            }

                            // поиск референса типа refFieldsJson того же класса, что и референс из атрибута
                            for (oneRefObject in oneLoadObjRef.filter { it.typeRef == "refFieldsJson" && it.code == oneRefFieldJson.codeRef }) {
                                if (oneRefObject.row.fields.find { it.fieldName == oneRefObject.fieldRef && it.fieldValue == fieldValueInJsonStr } != null) {
                                    // референс найден. ищу его ид в бд приемнике и заменяю им ид из бд источника
                                    val refFieldValueNew =
                                        loadObject.getObjIdInDB(
                                            oneRefObject.row.fields,
                                            oneRefFieldJsonClass,
                                            oneRefObject.nestedLevel,
                                            true
                                        )
                                    if (fieldValueInJsonStr != refFieldValueNew) {
                                        if (itemFromAttribute is ObjectNode) {
                                            (itemFromAttribute as ObjectNode).put(fieldNameInJsonStr, refFieldValueNew)
                                        } else if (attributeRecord is ObjectNode) {
                                            (attributeRecord as ObjectNode).put(fieldNameInJsonStr, refFieldValueNew)
                                        } else {
                                            (attributeRecord as ArrayNode).set(index, refFieldValueNew)
                                        }
                                    }
                                }
                            }
                            if (attributeRecord !is ArrayNode) {
                                break
                            }
                        }
                        oneLoadObjFields[indexOfJsonField].fieldValue =
                            WriterJson().createJsonFile(attribute as ObjectNode)
                    }
                }
            }
        }
    }


}