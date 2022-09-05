package com.solanteq.service

import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

// класс, в котором связан id объекта из базы источника с id объекта в базе приемнике
data class DataObjectDest(
    val code: String,  // название класса из конфигурации
    val id: String,         // значение поля refFieldId из базы источника
    val idDest: String,      // значение поля refFieldId из базы приемника
    val listChildRef: List<Fields> // значение референсных полей типа InChild
)

// класс используется для хранения данных о добавленных измеряемых величинах шкалы в рамках одного тарифа
data class ScaleAmountInDB(
    var fields: List<Fields>,
    var newId: String
)

//class LoadObject(allLoadObjectMain: DataDBMain) {
object LoadObject {

    //private val allLoadObject = allLoadObjectMain
    public lateinit var allLoadObject: DataDBMain

    // действие при загрузке для каждого объекта из файла: добавление/обновление/пропуск
    public lateinit var listOfActionWithObject: MutableList<ActionWithObject>

    // коннект к БД
    //public lateinit var conn: Connection
    //public val conn: Connection = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)

    /*// Объект из файла:
    // 0 - полностью совпадает с объектом из БД
    // 1 - подлежит замене в БД
    // 2 - добавлен в БД
    val OBJECT_TO_SKIP = 0
    val OBJECT_TO_UPDATE = 1
    val OBJECT_TO_INSERT = 2*/

    // считывание конфигурации
    private val readJsonFile = ReadJsonFile()
    private val jsonConfigFile = readJsonFile.readConfig()

    private val scalable = Scalable(jsonConfigFile)

    //private val readerDB = ReaderDB()

    //private val checkObject = CheckObject(allLoadObject)
    private val checkObject = CheckObject

    private val createReport = CreateReport()

    // список найденных объектов файла в БД приемнике
    private var dataObjectDestList = mutableListOf<DataObjectDest>()

    private val logger = LoggerFactory.getLogger(javaClass)

    //private var listNewScalableAmountObject = mutableListOf<ScaleAmountInDB>()

    fun loadDataObject() {

        // считывание конфигурации
        //val readJsonFile = ReadJsonFile()
        //val jsonConfigFile = readJsonFile.readConfig()
        logger.info("Configuration info: " + jsonConfigFile.cfgList[0])

        // формирования списка полей, которые не нужно выгружать, для каждого класса
        CommonFunctions().createListFieldsNotExport(jsonConfigFile)

        // считывание файла объектов
        //val allLoadObject = readJsonFile.readObject()

        // проверка версии и названия конфигурационного файла и файла с объектами
        if (jsonConfigFile.cfgList[0].version != allLoadObject.cfgList[0].version ||
            jsonConfigFile.cfgList[0].cfgName != allLoadObject.cfgList[0].cfgName
        ) {
            logger.error("Different versions or cfgName of Configuration File and Object File.")
            //exitProcess(-1)
            exitFromProgram()
        }

        // установка соединения с БД
        //conn = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)

        // цикл по главным объектам (главные объекты это объекты, которые нужно загрузить в БД).
        // если главный объект успешно загружен, то добавляю в список его полей Fields("@isLoad","1")
        for (oneLoadObject in allLoadObject.element) {

            // поиск описания класса референсного объекта в файле конфигурации
            val oneConfLoadObj = jsonConfigFile.objects.find { it.code == oneLoadObject.code }!!
            val chainLoadObject = mutableListOf<DataDB>(oneLoadObject)
            loadOneObject(
                chainLoadObject,
                oneConfLoadObj,
                jsonConfigFile,
                allLoadObject.element
            )
        }
        createQueryForFamilyObject(/*allLoadObject, jsonConfigFile*/)
        //conn.close()

        createReport.writeReportOfProgramExecution("load")
    }

    private fun loadOneObject(
        chainLoadObject: MutableList<DataDB>,
        oneConfClassObj: ObjectCfg,
        jsonConfigFile: RootCfg,
        allLoadObject: List<DataDB>
    ) {

        val oneLoadObject = chainLoadObject.last()

        // если у главного объекта есть референсы refObject, которые тоже являются главным объектом,
        // то сначала нужно найти эти референсы и загрузить их
        for (oneRefObject in oneLoadObject.row.refObject) {

            // поиск описания класса референсного объекта в файле конфигурации
            val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneRefObject.code }!!

            // поиск референсного объекта среди главных объектов.
            // если indexInAllLoadObject > -1, то референсный объект найден среди главных
            val indexInAllLoadObject =
                //CheckObject().findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)
                checkObject.findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)

            // нашли референсный объект среди главных объектов. теперь проверка референсов найденного главного объекта
            if (indexInAllLoadObject > -1) {

                // референсы подобного типа пропускаю, т.к. они заведомо закольцованы и при загрузке обрабатываются отдельным образом
                if (/*oneRefObject.typeRef.lowercase() == "inparent" ||*/ oneRefObject.typeRef.lowercase() == "inchild") {
                    continue
                }

                chainLoadObject.add(allLoadObject[indexInAllLoadObject])

                // рекурсивный поиск референсных объектов, которые есть среди главных объектов и их загрузка
                loadOneObject(
                    chainLoadObject,
                    oneConfClassRefObj,
                    jsonConfigFile,
                    allLoadObject
                )
            }
        }

        // если у главного объекта есть референсы linkObjects, которые тоже являются главным объектом,
        // то сначала нужно найти эти референсы и загрузить их
        for (oneLinkObject in oneLoadObject.row.linkObjects) {
            for (oneRefObject in oneLinkObject.row.refObject) {
                // поиск описания класса референсного объекта в файле конфигурации
                val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneRefObject.code }!!

                // поиск референсного объекта среди главных объектов.
                // если indexInAllLoadObject > -1, то референсный объект найден среди главных
                val indexInAllLoadObject =
                    //CheckObject().findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)
                    checkObject.findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)

                // нашли референсный объект среди главных объектов. теперь проверка референсов найденного главного объекта
                if (indexInAllLoadObject > -1) {

                    chainLoadObject.add(allLoadObject[indexInAllLoadObject])

                    // рекурсивный поиск референсных объектов, которые есть среди главных объектов и их загрузка
                    loadOneObject(
                        chainLoadObject,
                        oneConfClassRefObj,
                        jsonConfigFile,
                        allLoadObject
                    )
                }
            }

            // если у linkObjects есть свои linkObject со своими референсами, которые тоже являются главным объектом,
            // то сначала нужно найти эти референсы и загрузить их
            for (oneLinkObject2Lvl in oneLinkObject.row.linkObjects) {
                for (oneRefObject in oneLinkObject2Lvl.row.refObject) {
                    // поиск описания класса референсного объекта в файле конфигурации
                    val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneRefObject.code }!!

                    // поиск референсного объекта среди главных объектов.
                    // если indexInAllLoadObject > -1, то референсный объект найден среди главных
                    val indexInAllLoadObject =
                        //CheckObject().findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)
                        checkObject.findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)

                    // нашли референсный объект среди главных объектов. теперь проверка референсов найденного главного объекта
                    if (indexInAllLoadObject > -1) {

                        chainLoadObject.add(allLoadObject[indexInAllLoadObject])

                        // рекурсивный поиск референсных объектов, которые есть среди главных объектов и их загрузка
                        loadOneObject(
                            chainLoadObject,
                            oneConfClassRefObj,
                            jsonConfigFile,
                            allLoadObject
                        )
                    }
                }
            }

            /*for (oneScaleObject in oneLoadObject.row.scaleObjects) {
                for (oneRefObject in oneScaleObject.row.refObject) {
                    // поиск описания класса референсного объекта в файле конфигурации
                    val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneRefObject.code }!!

                    // поиск референсного объекта среди главных объектов.
                    // если indexInAllLoadObject > -1, то референсный объект найден среди главных
                    val indexInAllLoadObject =
                        //CheckObject().findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)
                        checkObject.findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)

                    // нашли референсный объект среди главных объектов. теперь проверка референсов найденного главного объекта
                    if (indexInAllLoadObject > -1) {

                        chainLoadObject.add(allLoadObject[indexInAllLoadObject])

                        // рекурсивный поиск референсных объектов, которые есть среди главных объектов и их загрузка
                        loadOneObject(
                            chainLoadObject,
                            oneConfClassRefObj,
                            jsonConfigFile,
                            allLoadObject
                        )
                    }
                }
            }*/
        }

        for (oneScaleObject in oneLoadObject.row.scaleObjects) {
            for (oneRefObject in oneScaleObject.row.refObject) {
                // поиск описания класса референсного объекта в файле конфигурации
                val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneRefObject.code }!!

                // поиск референсного объекта среди главных объектов.
                // если indexInAllLoadObject > -1, то референсный объект найден среди главных
                val indexInAllLoadObject =
                    //CheckObject().findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)
                    checkObject.findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)

                // нашли референсный объект среди главных объектов. теперь проверка референсов найденного главного объекта
                if (indexInAllLoadObject > -1) {

                    chainLoadObject.add(allLoadObject[indexInAllLoadObject])

                    // рекурсивный поиск референсных объектов, которые есть среди главных объектов и их загрузка
                    loadOneObject(
                        chainLoadObject,
                        oneConfClassRefObj,
                        jsonConfigFile,
                        allLoadObject
                    )
                }
            }
        }

        // процедура записи главного объекта в базу
        saveOneObject(oneLoadObject, oneConfClassObj, allLoadObject, jsonConfigFile)
    }

    // запись главного объекта в базу
    private fun saveOneObject(
        oneLoadObject: DataDB,
        oneConfClassObj: ObjectCfg,
        allLoadObject: List<DataDB>,
        jsonConfigFile: RootCfg
    ) {

        // если объект уже загружался, то его поле @isLoad=1
        if (Fields("@isLoad", "1") in oneLoadObject.row.fields) {
            return
        }

        // формирование запроса для изменения данных в базе
        createSaveObjSql(oneLoadObject, oneConfClassObj, allLoadObject, jsonConfigFile)

        oneLoadObject.row.fields += Fields("@isLoad", "1")
    }

    // формирование запроса для изменения данных в базе
    private fun createSaveObjSql(
        oneLoadObject: DataDB,
        oneConfClassObj: ObjectCfg,
        allLoadObject: List<DataDB>,
        jsonConfigFile: RootCfg
    ) {

        var sqlQueryObjMain: String = ""
        var sqlQueryRefTables: String = ""
        val sqlQuery: String
        val nextValueRevName = "nextValueRev"
        var sqlQueryInsertScale = ""

        var sqlQueryObjDeclare = ""
        var sqlQueryObjInit = ""
        var sqlQueryLinkObject = ""

        val actionWithObject =
            listOfActionWithObject.find { it.classCode == oneLoadObject.code && it.id == oneLoadObject.row.fields.find { fields -> fields.fieldName == oneConfClassObj.keyFieldIn }!!.fieldValue!! }

        // получаю идентификатор объекта в базе приемнике, либо генерю новый
        val idObjectInDB = getObjIdInDB(oneLoadObject.row.fields, oneConfClassObj, 0, true)

        // если у класса есть шкала, то работаем с ней
        if (scalable.isClassHaveScale(oneConfClassObj)) {

            // идентификатор шкалы в файле
            var idScaleInFile = ""

            // идентификатор шкалы в БД
            val idScaleInDB = findScaleIdInDB(oneConfClassObj, idObjectInDB)

            // добавляю новую шкалу в базу
            if (idScaleInDB == "" && isScaleExistsInfile(oneLoadObject)) {

                // ид шкалы в файле уже изменен на нужный. Получаю его
                idScaleInFile =
                    oneLoadObject.row.fields.find { it.fieldName == scalable.getScaleIdFieldName(oneLoadObject) }!!.fieldValue!!

                // проверка есть в CheckObject "There is no description of scale class in configuration file", поэтому здесь не упадет
                val scaleClass = scalable.getScaleConfigClassDescription()!!

                val oneScaleObject = DataDB(
                    scalable.getClassNameByScaleKeyType("InScale"),
                    oneLoadObject.loadMode,
                    Row(listOf<Fields>(), listOf<RefObject>(), listOf<DataDB>(), listOf<DataDB>())
                )
                sqlQueryInsertScale += "\n" +
                        createMainInsUpdQuery(
                            oneScaleObject,
                            scaleClass,
                            idScaleInFile,
                            "",
                            "insert"
                        )

                val nextValueRevScaleName = "nextValueRevScaleId"

                // объявление переменных для значений сиквенса в psql
                sqlQueryObjDeclare += "declare $nextValueRevScaleName integer; \n"
                // инициализация переменных для сиквенса в psql
                sqlQueryObjInit += "$nextValueRevScaleName=nextval('${CommonConstants().commonSequence}'); \n"

                // insert в таблицу аудита новой записи
                sqlQueryInsertScale +=
                    createMainInsUpdQuery(
                        oneScaleObject,
                        scaleClass,
                        idScaleInFile,
                        nextValueRevScaleName,
                        "insertAudValues"
                    ) + "\n"
            }
        }

        // формирование условий запроса update/insert главного объекта
        if (actionWithObject != null && !actionWithObject.actionSkip) {

            // формирование запроса для добавления/обновления основного объекта
            if (actionWithObject.actionUpdateMainRecord || actionWithObject.actionInsert) {

                if (actionWithObject.actionUpdateMainRecord) {
                    sqlQueryObjMain = createMainInsUpdQuery(oneLoadObject, oneConfClassObj, idObjectInDB, "", "update")
                } else {
                    sqlQueryObjMain = createMainInsUpdQuery(oneLoadObject, oneConfClassObj, idObjectInDB, "", "insert")
                }

                sqlQueryObjDeclare += "declare $nextValueRevName integer; \n"
                sqlQueryObjInit += "$nextValueRevName = nextval('${CommonConstants().commonSequence}'); \n"

                // формирование условий запроса insert в таблицу аудита главного объекта
                sqlQueryObjMain +=
                    createMainInsUpdQuery(
                        oneLoadObject,
                        oneConfClassObj,
                        idObjectInDB,
                        nextValueRevName,
                        "insertAudValues"
                    )
            }

            // формирование условий запроса для таблицы связей (референсы refTables)
            if (actionWithObject.actionUpdateRefTables || actionWithObject.actionInsert) {
                sqlQueryRefTables =
                    createRefTablesQuery(
                        oneLoadObject,
                        oneConfClassObj,
                        allLoadObject,
                        jsonConfigFile,
                        idObjectInDB,
                        "mainObject"
                    )
            }

            if (actionWithObject.actionUpdateLinkRecord || actionWithObject.actionInsert) {
                sqlQueryObjDeclare += actionWithObject.queryToUpdateLinkRecDeclare
                sqlQueryObjInit += actionWithObject.queryToUpdateLinkRecInit
                sqlQueryLinkObject += actionWithObject.queryToUpdateLinkRecObject
            }

            // запрос на изменение основного объекта, его refTables и linkObjects
            sqlQuery = "\nDO $$ \n" +
                    "declare revTimeStamp numeric(18,0); \n" +
                    "declare dateNow timestamp; \n" +
                    sqlQueryObjDeclare +
                    "BEGIN \n" +
                    "dateNow = now(); \n" +
                    "revTimeStamp = cast(extract(epoch from dateNow) * 1000 as numeric(18,0)); \n" +
                    sqlQueryObjInit +
                    sqlQueryInsertScale +
                    sqlQueryObjMain +
                    sqlQueryRefTables +
                    sqlQueryLinkObject +
                    "END $$; \n"

            createTranSaveObj(sqlQuery, oneLoadObject, oneConfClassObj)

        }

        logger.info(
            CommonFunctions().createObjectIdForLogMsg(
                oneConfClassObj.code,
                oneConfClassObj.keyFieldOut,
                oneLoadObject.row.fields,
                -1
            ) + if (actionWithObject!!.actionInsert)
                "The object has been inserted into the database. "
            else if (actionWithObject.actionUpdateMainRecord || actionWithObject.actionUpdateRefTables || actionWithObject.actionUpdateLinkRecord)
                "The object has been updated in the database. "
            else
                "The object has been skipped."
        )

        //createReportOfProgramExecution(oneConfClassObj.code, actionWithMainObject)
        createReport.createSummaryReport(oneConfClassObj.code, actionWithObject)

    }

    // внести изменение в базу в рамках транзакции
    private fun createTranSaveObj(
        sqlQuery: String,
        oneLoadObject: DataDB,
        oneConfClassObj: ObjectCfg
    ) {

        try {
            //val connect = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
            val connect = DatabaseConnection.getConnection(javaClass.toString(),oneConfClassObj.aliasDb)
            connect.autoCommit = false
            val queryStatement = connect.prepareStatement(sqlQuery)
            logger.trace(
                CommonFunctions().createObjectIdForLogMsg(
                    oneConfClassObj.code,
                    oneConfClassObj.keyFieldOut,
                    oneLoadObject.row.fields,
                    -1
                ) + "Query to load object into <${DatabaseConnection.getAliasConnection(oneConfClassObj.aliasDb)}> : $sqlQuery"
            // (connect as PgConnection).url - строка базы для подключения
            )
            queryStatement.executeUpdate()
            connect.commit()
            //connect.rollback()
            queryStatement.close()
            //connect.close()
        } catch (e: Exception) {
            logger.error(
                CommonFunctions().createObjectIdForLogMsg(
                    oneConfClassObj.code,
                    oneConfClassObj.keyFieldOut,
                    oneLoadObject.row.fields,
                    -1
                ) + "\n" + "<The error with query>. " + e.message
            )
            //exitProcess(-1)
            exitFromProgram()
        }
    }

    // формирование запроса на вставку данных в таблицы связи, описанные в параметре refTables файла конфигурации
    public fun createRefTablesQuery(
        oneLoadObject: DataDB,
        oneConfClassObj: ObjectCfg,
        allLoadObject: List<DataDB>?,
        jsonConfigFile: RootCfg,
        idObjectInDB: String,
        typeLoadObject: String // mainObject обработка refTables главного объекта, linkObject обработка refTables объекта linkObject
    ): String {

        var idRefObjectInDB: String
        var sqlQueryLinkTable = ""

        // все ссылки refTables из конфигурации
        for (oneConfLinkObj in oneConfClassObj.refTables) {
            // все референсы типа refTables того же класса, что и найденный в конфигурации
            for (oneRefTables in oneLoadObject.row.refObject.filter { it.typeRef == "refTables" && it.code == oneConfLinkObj.codeRef }) {

                val oneRefLinkClass = jsonConfigFile.objects.find { it.code == oneRefTables.code }!!

                if (allLoadObject != null && typeLoadObject == "mainObject") {
                    val indexInAllCheckObject =
                        //CheckObject().findRefObjAmongMainObj(oneRefTables, oneRefLinkClass, allLoadObject)
                        checkObject.findRefObjAmongMainObj(oneRefTables, oneRefLinkClass, allLoadObject)
                    if (indexInAllCheckObject > -1) {

                        val item = allLoadObject[indexInAllCheckObject]
                        idRefObjectInDB = getObjIdInDB(item.row.fields, oneRefLinkClass, 0, true)
                        sqlQueryLinkTable += "insert into ${oneConfLinkObj.table} (${oneConfLinkObj.refField},${oneConfLinkObj.refFieldTo}) " +
                                "values('$idObjectInDB','$idRefObjectInDB'); \n"
                    }
                } else {
                    idRefObjectInDB =
                        getObjIdInDB(oneRefTables.row.fields, oneRefLinkClass, oneRefTables.nestedLevel, true)
                    sqlQueryLinkTable += "insert into ${oneConfLinkObj.table} (${oneConfLinkObj.refField},${oneConfLinkObj.refFieldTo}) " +
                            "values('$idObjectInDB','$idRefObjectInDB'); \n"
                }
            }

            if (sqlQueryLinkTable != "" && typeLoadObject == "mainObject") {
                //Важно!!! Считаю, что нужно обновить все ссылки и при этом все необходимые ссылки есть в файле данных
                sqlQueryLinkTable =
                    "delete from ${oneConfLinkObj.table} where ${oneConfLinkObj.refField}='$idObjectInDB'; \n" + sqlQueryLinkTable
            }
        }

        return sqlQueryLinkTable

    }

    // формирование запроса insert/update для основной таблицы загружаемого объекта
    public fun createMainInsUpdQuery(
        oneLoadObject: DataDB?, // должно быть null только для insertAudSelectValue
        oneConfClassObj: ObjectCfg,
        idObjectInDB: String,
        nextValueRevName: String, // название переменной, в которой значение сиквенса commonSequence. только для работы с таблицей аудита (insertAudValues и insertAudSelectValue)
        regimQuery: String, // insert в основную таблицу;
        // update основной таблицы;
        // insertAudValues формирование запроса insert в таблицу аудита значений из файла;
        // insertAudSelectValue формирование запроса insert в таблицу аудита значений из таблицы;
    ): String {

        var sqlQuery: String
        var listColName = ""
        var listColValue = ""
        var listColNameColValue = ""
        var listColNameEqualValue = ""

        // формирование условий запроса update/insert
        if (oneLoadObject != null) {

            for (field in oneLoadObject.row.fields) {
                if (oneConfClassObj.fieldsNotExport.find { it.name == field.fieldName } == null &&
                    field.fieldName != oneConfClassObj.keyFieldIn) {
                    val string = "Kotlin is a programming language"
                    val substring = "programming language"
                    val found = string.findLastAnyOf(listOf(substring));

                    listColName += field.fieldName + ","
                    if (field.fieldValue == null) {
                        listColNameEqualValue += "am.${field.fieldName} is ${field.fieldValue} and "
                        listColNameColValue += "${field.fieldName}=${field.fieldValue}, "
                        listColValue += "${field.fieldValue},"
                    } else {

                        field.fieldValue = field.fieldValue!!.replace("'", "''")

                        listColNameEqualValue += "am.${field.fieldName} = '${field.fieldValue}' and "
                        listColNameColValue += "${field.fieldName}='${field.fieldValue}', "
                        listColValue += "'${field.fieldValue}',"
                    }
                }
            }
            //listColNameEqualValue = "am."+listColNameColValue.replace(", ", " and am.").substringBeforeLast("and am.")
            listColNameEqualValue = listColNameEqualValue.substringBeforeLast("and")

            listColNameColValue += "${oneConfClassObj.auditDateField}=dateNow,audit_user_id='42'"
            listColName += "${oneConfClassObj.auditDateField},audit_user_id,audit_state,${oneConfClassObj.keyFieldIn}"
            listColValue += "dateNow,'42','A',$idObjectInDB"
        }


        if (regimQuery == "insert") {

            sqlQuery = "insert into ${oneConfClassObj.tableName} ($listColName) " +
                    "values($listColValue); \n"

        } else if (regimQuery == "insertAudValues") {

            sqlQuery = "insert into revinfo (rev,rev_timestamp) select $nextValueRevName,revTimeStamp; \n"
            sqlQuery += "insert into ${oneConfClassObj.tableNameAud} ($listColName,rev,revtype) " +
                    "values($listColValue,$nextValueRevName,1); \n"

        } else if (regimQuery == "insertAudSelectValue") {

            // формирование условий запроса insert в таблицу аудита
            val sqlQueryLocal = "select column_name " +
                    "from information_schema.columns " +
                    "where table_name = '${oneConfClassObj.tableName}' and column_name not in('rev','revtype')" +
                    "order by ordinal_position"

            val conn = DatabaseConnection.getConnection(javaClass.toString(),oneConfClassObj.aliasDb)
            val queryStatement = conn.prepareStatement(sqlQueryLocal)
            val queryResult = queryStatement.executeQuery()
            // формирование массива из ResultSet
            val listColumnName = queryResult.use {
                generateSequence {
                    if (queryResult.next()) {
                        queryResult.getString(1).lowercase()
                    } else null
                }.toList()
            }
            queryResult.close()
            val listColumnNameStr = listColumnName.joinToString(separator = ",")

            sqlQuery = "insert into revinfo (rev,rev_timestamp) select $nextValueRevName,revTimeStamp; \n"
            sqlQuery += "insert into ${oneConfClassObj.tableNameAud} ($listColumnNameStr,rev,revtype) " +
                    "select $listColumnNameStr,$nextValueRevName,1 from ${oneConfClassObj.tableName} " +
                    "where ${oneConfClassObj.keyFieldIn}='$idObjectInDB'; \n"

        } else if (regimQuery == "selectScalableAmount") {
            sqlQuery = ""
            if (oneLoadObject != null) {

                val scaleCompClass = scalable.getClassDescriptionByCode(scalable.scaleComponentClassName)
                val scaleCompValueClass = scalable.getClassDescriptionByCode(scalable.scaleComponentValueClassName)
                val scalableAmountClass = scalable.getClassDescriptionByCode(scalable.scalableAmountClassName)

                sqlQuery = " select distinct am.${scalableAmountClass.keyFieldIn} " +
                        " from ${scaleCompClass.tableName} comp " +
                        " join ${scaleCompValueClass.tableName} val on val.${scalable.scaleComponentValueFieldName} = comp.${scaleCompClass.keyFieldIn} " +
                        " join ${scalableAmountClass.tableName} am on am.${scalableAmountClass.keyFieldIn} = val.${scalable.scalableAmountFieldName} " +
                        " where $listColNameEqualValue; \n"
            }
        }
        else if (regimQuery == "selectCountLinkObject") {
            sqlQuery = "select count(id) over() " +
                    "from ${oneConfClassObj.tableName} am " +
                    "where $listColNameEqualValue and audit_state='A';"
        }
        else {
            sqlQuery = "update ${oneConfClassObj.tableName} set $listColNameColValue " +
                    "where ${oneConfClassObj.keyFieldIn}='$idObjectInDB'; \n"

        }
        return sqlQuery
    }

    // получаю идентификатор объекта в базе приемнике, либо генерю новый
    public fun getObjIdInDB(
        objFieldsList: List<Fields>,
        oneConfigClass: ObjectCfg,
        nestedLevel: Int,
        isGenerateNewSeqValue: Boolean = true
    ): String {

        var idObjectInDB = "-"
        val refFieldIdValue = objFieldsList.find { it.fieldName == oneConfigClass.keyFieldIn }!!.fieldValue!!

        // поиск объекта в списке ранее найденных объектов в бд приемнике
        val dataObjectDest = dataObjectDestList.find { it.code == oneConfigClass.code && it.id == refFieldIdValue }
        dataObjectDest?.let {
            idObjectInDB = it.idDest
            return idObjectInDB
        }

        //val checkObject = CheckObject()

        // поиск объекта в БД
        idObjectInDB = checkObject.findObjectInDB(objFieldsList, oneConfigClass, -1)

        // генерация нового значения id, если объект не был найден в бд
        if (idObjectInDB == "-" && isGenerateNewSeqValue) {
            idObjectInDB = nextSequenceValue(oneConfigClass)
        }

        // добавляю объект в список уже найденных в бд приемнике
        if (idObjectInDB != "-" &&
            //!dataObjectDestList.contains(DataObjectDest(oneConfigClass.code, refFieldIdValue, idObjectInDB))
            dataObjectDestList.find { it.code == oneConfigClass.code && it.id == refFieldIdValue && it.idDest == idObjectInDB } == null
        ) {
            val listChildRef = mutableListOf<Fields>()
            if (oneConfigClass.refObjects.find { it.keyType.lowercase() == "inchild" } != null && nestedLevel < 2) {
                for (oneRefObjInChildDescr in oneConfigClass.refObjects.filter { it.keyType.lowercase() == "inchild" }) {
                    if (objFieldsList.find { it.fieldName == oneRefObjInChildDescr.refField }!!.fieldValue != null) {
                        listChildRef.add(
                            Fields(
                                oneRefObjInChildDescr.refField,
                                objFieldsList.find { it.fieldName == oneRefObjInChildDescr.refField }!!.fieldValue!!
                            )
                        )
                    }
                }
            }
            // запоминаю только главные объекты и референсы 1 уровня. референсы 2 уровня запоминаю только если у их класса нет рефернсов inchild
            if (nestedLevel < 2 || (oneConfigClass.refObjects.find { it.keyType.lowercase() == "inchild" } == null && nestedLevel == 2)) {
                dataObjectDestList.add(DataObjectDest(oneConfigClass.code, refFieldIdValue, idObjectInDB, listChildRef))
            }
        }

        return idObjectInDB
    }

    // получить следующее значение последовательности postgresql
    public fun nextSequenceValue(oneConfClassObj: ObjectCfg): String {

        val sequence = oneConfClassObj.sequence

        val sqlQuery = "SELECT nextval('${sequence}')"
        //val connNextSequence = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
        val connNextSequence = DatabaseConnection.getConnection(javaClass.toString(),oneConfClassObj.aliasDb)
        val queryStatement = connNextSequence.prepareStatement(sqlQuery)
        val queryResult = queryStatement.executeQuery()

        queryResult.next()
        val nextValue = queryResult.getString(1)
        queryResult.close()
        //connNextSequence.close()

        return nextValue
    }

    // создание запроса для изменения полей референсов типа inchild
    private fun createQueryForFamilyObject(
        //allLoadObject: DataDBMain
        //jsonConfigFile: RootCfg
    ) {
        // цикл по классам в схеме
        for (oneConfigClassDescr in jsonConfigFile.objects) {
            // цикл по описаниям референсов типа inchild в схеме
            for (oneRefObjInChildDescr in oneConfigClassDescr.refObjects.filter { it.keyType.lowercase() == "inchild" }) {
                // цикл по загруженным объектам того класса, в котором есть референс типа inchild
                for (oneLoadObject in allLoadObject.element.filter { it.code == oneConfigClassDescr.code }) {

                    // старый(из бд источника) и новый(в бд приемнике) id загружаемого объекта
                    val oneLoadObjIdOld =
                        oneLoadObject.row.fields.find { it.fieldName == oneConfigClassDescr.keyFieldIn }!!.fieldValue
                    val oneLoadObjIdNew =
                        dataObjectDestList.find { it.code == oneConfigClassDescr.code && it.id == oneLoadObjIdOld }!!.idDest

                    // класс ссылочного поля типа inchild
                    val oneConfClassRefInChild =
                        jsonConfigFile.objects.find { it.code == oneRefObjInChildDescr.codeRef }!!

                    // текущее значение ссылочного поля типа inchild в базе
                    // если refFieldValueOld = null, то в файле ссылочное поле не заполнено
                    val refFieldValueOld =
                        dataObjectDestList.find { it.code == oneConfigClassDescr.code && it.id == oneLoadObjIdOld }!!.listChildRef.find { it.fieldName == oneRefObjInChildDescr.refField }?.fieldValue
                    //oneLoadObject.row.fields.find { it.fieldName == oneRefObjInChildDescr.refField }!!.fieldValue

                    // новое значение id объекта из поля типа inchild. если refFieldValueNew = null, значит объект уже был в базе и значение ссылочного поля в бд правильное
                    val refFieldValueNew =
                        dataObjectDestList.find { it.code == oneConfClassRefInChild.code && it.id == refFieldValueOld }?.idDest

                    if (refFieldValueNew != null && refFieldValueOld != null && refFieldValueOld != refFieldValueNew) {

                        val indexOfRefField =
                            oneLoadObject.row.fields.indexOfFirst { it.fieldName == oneRefObjInChildDescr.refField }
                        oneLoadObject.row.fields[indexOfRefField].fieldValue = refFieldValueNew

                        val nextValueRevChildName = "nextValueChildId"

                        // объявление переменных для значений сиквенса в psql
                        var sqlQuery = "\nDO $$ \n" +
                                "declare revTimeStamp numeric(18,0); \n" +
                                "declare dateNow timestamp; \n" +
                                "declare $nextValueRevChildName integer; \n" +
                                "BEGIN \n" +
                                "dateNow = now(); \n" +
                                "revTimeStamp = cast(extract(epoch from dateNow) * 1000 as numeric(18,0)); \n"
                        sqlQuery += createMainInsUpdQuery(
                            oneLoadObject,
                            oneConfigClassDescr,
                            oneLoadObjIdNew,
                            "",
                            "update"
                        )

                        // инициализация переменных для сиквенса в psql
                        sqlQuery += "$nextValueRevChildName=nextval('${CommonConstants().commonSequence}'); \n"

                        // insert в таблицу аудита обновленной записи
                        sqlQuery +=
                            createMainInsUpdQuery(
                                oneLoadObject,
                                oneConfigClassDescr,
                                oneLoadObjIdNew,
                                nextValueRevChildName,
                                "insertAudValues"
                            )
                        sqlQuery += "END $$; \n"

                        /*sqlQuery =
                            "update ${oneConfigClassDescr.tableName} set ${oneRefObjInChildDescr.refField}=$refFieldValueNew " +
                                    " where ${oneConfigClassDescr.keyFieldIn}=$oneLoadObjIdNew"*/

                        val conn = DatabaseConnection.getConnection(javaClass.toString(),oneConfigClassDescr.aliasDb)
                        val queryStatement = conn.prepareStatement(sqlQuery)
                        logger.trace(
                            CommonFunctions().createObjectIdForLogMsg(
                                oneConfigClassDescr.code,
                                oneConfigClassDescr.keyFieldOut,
                                oneLoadObject.row.fields,
                                -1
                            ) + "Query to update reference field with <keyType=InChild> field ${oneRefObjInChildDescr.refField}: $sqlQuery"
                        )
                        queryStatement.executeUpdate()

                        /*val connect = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
                        connect.autoCommit = false
                        val queryStatement = connect.prepareStatement(sqlQuery)
                        logger.trace(
                            CommonFunctions().createObjectIdForLogMsg(
                                oneConfClassObj.code,
                                oneConfClassObj.keyFieldOut,
                                oneLoadObject.row.fields,
                                -1
                            ) + "Query to load object: $sqlQuery"
                        )
                        queryStatement.executeUpdate()
                        connect.commit()
                        //connect.rollback()
                        connect.close()*/


                    }
                }
            }
        }
    }

    // знаю ид тарифа в БД приемнике. ищу связанную с ним шкалу
    public fun findScaleIdInDB(
        oneConfClassObj: ObjectCfg,
        idObjectInDB: String
    ): String {

        var idScaleInDB = ""

        // если закачиваемый тариф есть в БД приемнике, то беру ид шкалы у него
        val connCompare = DatabaseConnection.getConnection(javaClass.toString(),oneConfClassObj.aliasDb)
        //val connCompare = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
        val queryStatement =
            connCompare.prepareStatement("select ${scalable.getScaleIdFieldName(oneConfClassObj)} from ${oneConfClassObj.tableName} where ${oneConfClassObj.keyFieldIn}=$idObjectInDB")

        val queryResult = queryStatement.executeQuery()

        while (queryResult.next()) {
            if (queryResult.getString(1) != null) {
                idScaleInDB = queryResult.getString(1)
            }
        }
        queryStatement.close()
        //connCompare.close()
        return idScaleInDB
    }

    // Проверка есть ли шкала у тарифа в файле
    // Если есть то вернут true, иначе false
    private fun isScaleExistsInfile(
        oneLoadObject: DataDB,
    ): Boolean {

        oneLoadObject.row.refObject.find { it.typeRef.lowercase() == "inscale" }?.let { return true }

        return false
    }

    private fun exitFromProgram() {
        createReport.writeReportOfProgramExecution("load")
        logger.info("The object file has been uploaded with error")
        exitProcess(-1)
    }

}