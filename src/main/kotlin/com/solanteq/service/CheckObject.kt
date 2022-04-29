package com.solanteq.service

import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import kotlin.system.exitProcess

class CheckObject {

    private val logger = LoggerFactory.getLogger(javaClass)

    public var allCheckObject = DataDBMain(listOf<CfgList>(), listOf<DataDB>())
    public var jsonConfigFile = RootCfg(listOf<CfgList>(), listOf<ObjectCfg>())

    // коннект к БД
    lateinit var conn: Connection

    fun checkDataObject() {

        // считывание конфигурации
        val readJsonFile = ReadJsonFile()
        jsonConfigFile = readJsonFile.readConfig()
        //val jsonConfigFile1 = configJson.readJsonFile<RootCfg>(CONFIG_FILE)
        logger.info("Configuration info: " + jsonConfigFile.cfgList[0])

        // формирования списка полей, которые не нужно выгружать, для каждого класса
        CommonFunctions().createListFieldsNotExport(jsonConfigFile)

        // считывание файла объектов
        allCheckObject = readJsonFile.readObject()

        // проверка версии и названия конфигурационного файла и файла с объектами
        if (jsonConfigFile.cfgList[0].version != allCheckObject.cfgList[0].version ||
            jsonConfigFile.cfgList[0].cfgName != allCheckObject.cfgList[0].cfgName
        ) {
            logger.error("Different versions or cfgName of Configuration File and Object File.")
            exitProcess(-1)
        }

        // установка соединения с БД
        conn = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
        //logger.info("DataBase connection parameters: dbconn=$CONN_STRING dbuser=$CONN_LOGIN dbpass=$CONN_PASS.")

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
        }
        conn.close()
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
            LoadObject().setNewIdFieldJson(
                oneCheckRefObj.row.fields,
                oneCheckRefObj.refObject,
                oneConfClassRefObj,
                jsonConfigFile
            )

            // установка нового значения ссылочного поля в файле для референса типа refFields
            LoadObject().setNewIdRefFields(
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
                    ) + "<RefTables links don't match with object from DB. "
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

        // запрос на поиск объекта
        val sqlQuery = createSqlQueryToFindObj(objFieldsList, oneConfigClass)
        logger.debug(
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
        logger.debug(
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
    private fun checkRefFieldValue(
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
        logger.debug(
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
                // -название колонки из таблицы не найдено среди референсов И
                // -название колонки из таблицы не найдено среди полей, которые не нужно проверять И
                // -в поля исключения добавляю keyFieldIn (id) И
                // -значение колонки из таблицы не равно значению поля из файла
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

                // если в цепочке референсов повторно встретили объект, то ошибка
                if (chainCheckObject.contains(allCheckObject[indexInAllCheckObject]) && oneRefObject.typeRef.lowercase() != "inparent" && oneRefObject.typeRef.lowercase() != "inchild") {
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
                } else if (chainCheckObject.contains(allCheckObject[indexInAllCheckObject]) && (oneRefObject.typeRef.lowercase() == "inparent" || oneRefObject.typeRef.lowercase() == "inchild")) {
                    // референсы подобного типа пропускаю, т.к. они заведомо закольцованы и при загрузке обрабатываются отдельным образом
                    continue
                }

                chainCheckObject.add(allCheckObject[indexInAllCheckObject])

                // рекурсивный поиск референсных объектов, которые есть среди главных объектов
                checkRingReference(chainCheckObject, oneConfClassRefObj, jsonConfigFile, allCheckObject)
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
            logger.debug(
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

}