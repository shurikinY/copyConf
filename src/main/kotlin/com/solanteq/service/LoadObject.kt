package com.solanteq.service

import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

// класс, в котором связан id объекта из базы источника с id объекта в базе приемнике
data class DataObjectDest(
    val code: String,  // название класса из конфигурации
    val id: String,         // значение поля refFieldId из базы источника
    val idDest: String,      // значение поля refFieldId из базы приемника
    var listChildRef: List<Fields> // значение референсных полей типа InChild
)

// класс используется для хранения данных о добавленных измеряемых величинах шкалы в рамках одного тарифа
data class ScaleAmountInDB(
    var fields: List<Fields>,
    var newId: String
)

object LoadObject {

    public lateinit var allLoadObject: DataDBMain

    // действие при загрузке для каждого объекта из файла: добавление/обновление/пропуск
    public lateinit var listOfActionWithObject: MutableList<ActionWithObject>

    // считывание конфигурации
    private val readJsonFile = ReadJsonFile()
    private val jsonConfigFile = readJsonFile.readConfig()

    private val scalable = Scalable(jsonConfigFile)

    private val checkObject = CheckObject

    private val createReport = CreateReport()

    // Список идентификаторов объектов из файла и соответствующих им идентификаторов в БД приемнике
    private var dataObjectDestList = mutableListOf<DataObjectDest>()

    private val logger = LoggerFactory.getLogger(javaClass)

    fun getDataObjectDestList(): List<DataObjectDest> {
        return dataObjectDestList
    }

    fun addDataObjectDestList(dataObject: DataObjectDest) {
        dataObjectDestList.add(dataObject)
    }

    fun loadDataObject() {

        // считывание конфигурации
        logger.info("Configuration info: " + jsonConfigFile.cfgList[0])

        // формирования списка полей, которые не нужно выгружать, для каждого класса
        CommonFunctions().createListFieldsNotExport(jsonConfigFile)

        // проверка версии и названия конфигурационного файла и файла с объектами
        if (jsonConfigFile.cfgList[0].version != allLoadObject.cfgList[0].version ||
            jsonConfigFile.cfgList[0].cfgName != allLoadObject.cfgList[0].cfgName
        ) {
            logger.error("Different versions or cfgName of Configuration File and Object File.")
            exitFromProgram()
        }

        // Цикл по главным объектам (главные объекты это объекты, которые нужно загрузить в БД).
        // Если главный объект успешно загружен, то добавляю в список его полей Fields("@isLoad","1")
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
        createQueryForFamilyObject()

        createReport.writeReportOfProgramExecution("load")
    }

    private fun loadOneObject(
        chainLoadObject: MutableList<DataDB>,
        oneConfClassObj: ObjectCfg,
        jsonConfigFile: RootCfg,
        allLoadObject: List<DataDB>
    ) {

        val oneLoadObject = chainLoadObject.last()

        // Если у главного объекта есть референсы refObject, которые тоже являются главным объектом,
        // то сначала нужно найти эти референсы и загрузить их
        for (oneRefObject in oneLoadObject.row.refObject) {

            // Поиск описания класса референсного объекта в файле конфигурации
            val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneRefObject.code }!!

            // Поиск референсного объекта среди главных объектов.
            // Если indexInAllLoadObject > -1, то референсный объект найден среди главных
            val indexInAllLoadObject =
                checkObject.findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)

            // Нашли референсный объект среди главных объектов. Теперь проверка референсов найденного главного объекта
            if (indexInAllLoadObject > -1) {

                val refKeyType = checkObject.getRefKeyType(oneConfClassRefObj, oneRefObject)

                // Референсы подобного типа пропускаю, т.к. они заведомо закольцованы и при загрузке обрабатываются отдельным образом
                if (oneRefObject.typeRef.lowercase() == "inchild" || refKeyType.equals("InCycle",true)) {
                    continue
                }

                chainLoadObject.add(allLoadObject[indexInAllLoadObject])

                // Рекурсивный поиск референсных объектов, которые есть среди главных объектов и их загрузка
                loadOneObject(
                    chainLoadObject,
                    oneConfClassRefObj,
                    jsonConfigFile,
                    allLoadObject
                )
            }
        }

        // Если у главного объекта есть референсы linkObjects, которые тоже являются главным объектом,
        // то сначала нужно найти эти референсы и загрузить их
        for (oneLinkObject in oneLoadObject.row.linkObjects) {
            for (oneRefObject in oneLinkObject.row.refObject) {
                // Поиск описания класса референсного объекта в файле конфигурации
                val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneRefObject.code }!!

                // Поиск референсного объекта среди главных объектов.
                // Если indexInAllLoadObject > -1, то референсный объект найден среди главных
                val indexInAllLoadObject =
                    checkObject.findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)

                // Нашли референсный объект среди главных объектов. Теперь проверка референсов найденного главного объекта
                if (indexInAllLoadObject > -1) {

                    chainLoadObject.add(allLoadObject[indexInAllLoadObject])

                    // Рекурсивный поиск референсных объектов, которые есть среди главных объектов и их загрузка
                    loadOneObject(
                        chainLoadObject,
                        oneConfClassRefObj,
                        jsonConfigFile,
                        allLoadObject
                    )
                }
            }

            // Если у linkObjects есть свои linkObject со своими референсами, которые тоже являются главным объектом,
            // то сначала нужно найти эти референсы и загрузить их
            for (oneLinkObject2Lvl in oneLinkObject.row.linkObjects) {
                for (oneRefObject in oneLinkObject2Lvl.row.refObject) {
                    // Поиск описания класса референсного объекта в файле конфигурации
                    val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneRefObject.code }!!

                    // Поиск референсного объекта среди главных объектов.
                    // Если indexInAllLoadObject > -1, то референсный объект найден среди главных
                    val indexInAllLoadObject =
                        checkObject.findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)

                    // Нашли референсный объект среди главных объектов. Теперь проверка референсов найденного главного объекта
                    if (indexInAllLoadObject > -1) {

                        chainLoadObject.add(allLoadObject[indexInAllLoadObject])

                        // Рекурсивный поиск референсных объектов, которые есть среди главных объектов и их загрузка
                        loadOneObject(
                            chainLoadObject,
                            oneConfClassRefObj,
                            jsonConfigFile,
                            allLoadObject
                        )
                    }
                }
            }
        }

        for (oneScaleObject in oneLoadObject.row.scaleObjects) {
            for (oneRefObject in oneScaleObject.row.refObject) {
                // Поиск описания класса референсного объекта в файле конфигурации
                val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneRefObject.code }!!

                // Поиск референсного объекта среди главных объектов.
                // Если indexInAllLoadObject > -1, то референсный объект найден среди главных
                val indexInAllLoadObject =
                    checkObject.findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)

                // Нашли референсный объект среди главных объектов. Теперь проверка референсов найденного главного объекта
                if (indexInAllLoadObject > -1) {

                    chainLoadObject.add(allLoadObject[indexInAllLoadObject])

                    // Рекурсивный поиск референсных объектов, которые есть среди главных объектов и их загрузка
                    loadOneObject(
                        chainLoadObject,
                        oneConfClassRefObj,
                        jsonConfigFile,
                        allLoadObject
                    )
                }
            }
        }

        // Процедура записи главного объекта в базу
        saveOneObject(oneLoadObject, oneConfClassObj, jsonConfigFile)
    }

    // Запись главного объекта в базу
    private fun saveOneObject(
        oneLoadObject: DataDB,
        oneConfClassObj: ObjectCfg,
        jsonConfigFile: RootCfg
    ) {

        // Если объект уже загружался, то его поле @isLoad=1
        if (Fields("@isLoad", "1") in oneLoadObject.row.fields) {
            return
        }

        // Формирование запроса для изменения данных в базе
        createSaveObjSql(oneLoadObject, oneConfClassObj, jsonConfigFile)

        oneLoadObject.row.fields += Fields("@isLoad", "1")
    }

    // Формирование запроса для изменения данных в базе
    private fun createSaveObjSql(
        oneLoadObject: DataDB,
        oneConfClassObj: ObjectCfg,
        jsonConfigFile: RootCfg
    ) {

        var sqlQueryObjMain = ""
        var sqlQueryRefTables = ""
        val sqlQuery: String
        val nextValueRevName = "nextValueRev"
        var sqlQueryInsertScale = ""

        var sqlQueryObjDeclare = ""
        var sqlQueryObjInit = ""
        var sqlQueryLinkObject = ""

        val actionWithObject =
            listOfActionWithObject.find { it.classCode == oneLoadObject.code && it.id == oneLoadObject.row.fields.find { fields -> fields.fieldName == oneConfClassObj.keyFieldIn }!!.fieldValue!! }

        // Получаю идентификатор объекта в базе приемнике, либо генерю новый

        val idObjectInDB = getObjIdInDB(oneLoadObject.row.fields, oneConfClassObj, 0, true)

        // Если у класса есть шкала, то работаем с ней
        if (scalable.isClassHaveScale(oneConfClassObj)) {

            // Идентификатор шкалы в файле
            var idScaleInFile = ""

            // Идентификатор шкалы в БД
            val idScaleInDB = findScaleIdInDB(oneConfClassObj, idObjectInDB)

            // Добавляю новую шкалу в базу
            if (idScaleInDB == "" && isScaleExistsInfile(oneLoadObject)) {

                // Ид шкалы в файле уже изменен на нужный. Получаю его
                idScaleInFile =
                    oneLoadObject.row.fields.find { it.fieldName == scalable.getScaleIdFieldName(oneLoadObject) }!!.fieldValue!!

                // Проверка есть в CheckObject "There is no description of scale class in configuration file", поэтому здесь не упадет
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

                // Объявление переменных для значений сиквенса в psql
                sqlQueryObjDeclare += "declare $nextValueRevScaleName integer; \n"
                // Инициализация переменных для сиквенса в psql
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

        // Формирование условий запроса update/insert главного объекта
        if (actionWithObject != null && !actionWithObject.actionSkip) {

            // Формирование запроса для добавления/обновления основного объекта
            if (actionWithObject.actionUpdateMainRecord || actionWithObject.actionInsert) {

                if (actionWithObject.actionUpdateMainRecord) {
                    sqlQueryObjMain = createMainInsUpdQuery(oneLoadObject, oneConfClassObj, idObjectInDB, "", "update")
                } else {
                    sqlQueryObjMain = createMainInsUpdQuery(oneLoadObject, oneConfClassObj, idObjectInDB, "", "insert")
                }

                sqlQueryObjDeclare += "declare $nextValueRevName integer; \n"
                sqlQueryObjInit += "$nextValueRevName = nextval('${CommonConstants().commonSequence}'); \n"

                // Формирование условий запроса insert в таблицу аудита главного объекта
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

            // Запрос на изменение основного объекта, его refTables и linkObjects
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

        createReport.createSummaryReport(oneConfClassObj.code, actionWithObject)

    }

    // Внести изменение в базу в рамках транзакции
    private fun createTranSaveObj(
        sqlQuery: String,
        oneLoadObject: DataDB,
        oneConfClassObj: ObjectCfg
    ) {
        try {
            val connect = DatabaseConnection.getConnection(javaClass.toString(), oneConfClassObj.aliasDb)
            connect.autoCommit = false
            val queryStatement = connect.prepareStatement(sqlQuery)
            logger.trace(
                CommonFunctions().createObjectIdForLogMsg(
                    oneConfClassObj.code,
                    oneConfClassObj.keyFieldOut,
                    oneLoadObject.row.fields,
                    -1
                ) + "Query to load object into <${DatabaseConnection.getAliasConnection(oneConfClassObj.aliasDb)}> : $sqlQuery"
            )
            queryStatement.executeUpdate()
            connect.commit()
            //connect.rollback()
            queryStatement.close()
        } catch (e: Exception) {
            logger.error(
                CommonFunctions().createObjectIdForLogMsg(
                    oneConfClassObj.code,
                    oneConfClassObj.keyFieldOut,
                    oneLoadObject.row.fields,
                    -1
                ) + "\n" + "<The error with query>. " + e.message
            )
            exitFromProgram()
        }
    }

    // Формирование запроса на вставку данных в таблицы связи, описанные в параметре refTables файла конфигурации.
    public fun createRefTablesQuery(
        oneLoadObject: DataDB,
        oneConfClassObj: ObjectCfg,
        jsonConfigFile: RootCfg,
        idObjectInDB: String,
        typeLoadObject: String // mainObject обработка refTables главного объекта, linkObject обработка refTables объекта linkObject
    ): String {

        var idRefObjectInDB: String
        var sqlQueryLinkTable = ""

        // Все ссылки refTables из конфигурации
        for (oneConfLinkObj in oneConfClassObj.refTables) {
            // Все референсы типа refTables того же класса, что и найденный в конфигурации
            for (oneRefTables in oneLoadObject.row.refObject.filter { it.typeRef == "refTables" && it.code == oneConfLinkObj.codeRef }) {

                val oneRefLinkClass = jsonConfigFile.objects.find { it.code == oneRefTables.code }!!

                idRefObjectInDB =
                    getObjIdInDB(oneRefTables.row.fields, oneRefLinkClass, oneRefTables.nestedLevel, true)
                sqlQueryLinkTable += "insert into ${oneConfLinkObj.table} (${oneConfLinkObj.refField},${oneConfLinkObj.refFieldTo}) " +
                        "values('$idObjectInDB','$idRefObjectInDB'); \n"
            }

            // Для главного объекта нужно обновить все ссылки refTables: удалить старые связи и добавить новые для всех референсов типа refTables
            // Для linkObject нужно обновить все ссылки refTables: нужно только добавить новые связи для всех референсов типа refTables.
            //   Старые связи для linkObject удалять не нужно, т.к. в случае отличия референсов старый linkObject в БД
            //   будет удален("audit_state=R/valid_to=current_date-1/будет отвязан от главного объекта для InRefFieldsJson") и добавится новый.
            //   К новому linkObject будут привязаны все refTables из файла.
            if (sqlQueryLinkTable != "" && typeLoadObject == "mainObject") {
                sqlQueryLinkTable =
                    "delete from ${oneConfLinkObj.table} where ${oneConfLinkObj.refField}='$idObjectInDB'; \n" + sqlQueryLinkTable
            }
        }

        return sqlQueryLinkTable

    }

    // Формирование запроса insert/update для основной таблицы загружаемого объекта
    public fun createMainInsUpdQuery(
        oneLoadObject: DataDB?, // Должно быть null только для insertAudSelectValue
        oneConfClassObj: ObjectCfg,
        idObjectInDB: String,
        nextValueRevName: String, // Название переменной, в которой значение сиквенса commonSequence. Только для работы с таблицей аудита (insertAudValues и insertAudSelectValue)
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

            val conn = DatabaseConnection.getConnection(javaClass.toString(), oneConfClassObj.aliasDb)
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
        } else if (regimQuery == "selectCountLinkObject") {
            sqlQuery = "select count(id) over() " +
                    "from ${oneConfClassObj.tableName} am " +
                    "where $listColNameEqualValue and audit_state='A';"
        } else {
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

        // Поиск в файле у загружаемого объекта идентификатора референса типа keyType=InChild
        // Один и тот же референс может быть описан с keyType=InChild и keyType=In
        val listChildRef = getInChildIdForInParentObject(objFieldsList, oneConfigClass, refFieldIdValue, nestedLevel)

        // Поиск объекта в списке ранее найденных объектов в БД приемнике.
        // Если объект найден, то выход и возврат идентификатора объекта в БД приемнике.
        val dataObjectDest = dataObjectDestList.find { it.code == oneConfigClass.code && it.id == refFieldIdValue }
        if (dataObjectDest != null) {
            if (listChildRef.isNotEmpty() && dataObjectDest.listChildRef.isEmpty()) {
                // Запись идентификатора референса keyType=InChild вместе с идентификатором его родителя.
                // Например, для объекта classifierType будет запомнен идентификатор его референса classifierValue в следующем виде:
                //   DataObjectDest(code=classifierType, id=100009, idDest=100582, listChildRef=[Fields(fieldName=default_value_id, fieldValue=100018)])
                //   Где 100009/100582 ид объекта classifierType в файле/в БД, 100018 ид объекта classifierValue из файла
                dataObjectDest.listChildRef = listChildRef
            }
            return dataObjectDest.idDest
        }

        // поиск объекта в БД
        idObjectInDB = checkObject.findObjectInDB(objFieldsList, oneConfigClass, -1)

        // генерация нового значения id, если объект не был найден в бд
        if (idObjectInDB == "-" && isGenerateNewSeqValue) {
            idObjectInDB = nextSequenceValue(oneConfigClass)
        }

        // Добавление в список пары идентификаторов: из файла/ из БД приемника
        if (idObjectInDB != "-") {
            dataObjectDestList.add(DataObjectDest(oneConfigClass.code, refFieldIdValue, idObjectInDB, listChildRef))
        }

        return idObjectInDB
    }

//  26.04.2023 ПОКА НЕ УДАЛЯТЬ
//    // получаю идентификатор объекта в базе приемнике, либо генерю новый
//    public fun getObjIdInDB(
//        objFieldsList: List<Fields>,
//        oneConfigClass: ObjectCfg,
//        nestedLevel: Int,
//        isGenerateNewSeqValue: Boolean = true
//    ): String {
//
//        var idObjectInDB = "-"
//        val refFieldIdValue = objFieldsList.find { it.fieldName == oneConfigClass.keyFieldIn }!!.fieldValue!!
//
//        // поиск объекта в списке ранее найденных объектов в бд приемнике
//        val dataObjectDest = dataObjectDestList.find { it.code == oneConfigClass.code && it.id == refFieldIdValue }
//        dataObjectDest?.let {
//            idObjectInDB = it.idDest
//            return idObjectInDB
//        }
//
//        // поиск объекта в БД
//        idObjectInDB = checkObject.findObjectInDB(objFieldsList, oneConfigClass, -1)
//
//        // генерация нового значения id, если объект не был найден в бд
//        if (idObjectInDB == "-" && isGenerateNewSeqValue) {
//            idObjectInDB = nextSequenceValue(oneConfigClass)
//        }
//
//        if (idObjectInDB != "-" &&
//            dataObjectDestList.find { it.code == oneConfigClass.code && it.id == refFieldIdValue && it.idDest == idObjectInDB } == null
//        ) {
//            val listChildRef = getInChildIdForInParentObject(objFieldsList, oneConfigClass, refFieldIdValue, nestedLevel)
//            // Запоминаю только главные объекты и референсы 1 уровня. Референсы 2 уровня запоминаю только если у их класса нет референсов inchild.
//            if (nestedLevel < 2 || (oneConfigClass.refObjects.find { it.keyType.lowercase() == "inchild" } == null && nestedLevel == 2)) {
//                dataObjectDestList.add(DataObjectDest(oneConfigClass.code, refFieldIdValue, idObjectInDB, listChildRef))
//            }
//        }
//
//        return idObjectInDB
//    }

    // получить следующее значение последовательности postgresql
    public fun nextSequenceValue(oneConfClassObj: ObjectCfg): String {

        val sequence = oneConfClassObj.sequence

        val sqlQuery = "SELECT nextval('${sequence}')"
        val connNextSequence = DatabaseConnection.getConnection(javaClass.toString(), oneConfClassObj.aliasDb)
        val queryStatement = connNextSequence.prepareStatement(sqlQuery)
        val queryResult = queryStatement.executeQuery()

        queryResult.next()
        val nextValue = queryResult.getString(1)
        queryResult.close()

        return nextValue
    }

    // Создание запроса для изменения полей референсов типа InChild
    private fun createQueryForFamilyObject(
    ) {
        // Цикл по классам в схеме
        for (oneConfigClassDescr in jsonConfigFile.objects) {
            // Цикл по описаниям референсов типа InChild в схеме
            for (oneRefObjInChildDescr in oneConfigClassDescr.refObjects.filter { it.keyType.lowercase() == "inchild" }) {
                // Цикл по загруженным объектам того класса, в котором есть референс типа InChild
                for (oneLoadObject in allLoadObject.element.filter { it.code == oneConfigClassDescr.code }) {

                    // Старый(из бд источника) и новый(в бд приемнике) id загружаемого объекта
                    val oneLoadObjIdOld =
                        oneLoadObject.row.fields.find { it.fieldName == oneConfigClassDescr.keyFieldIn }!!.fieldValue
                    val oneLoadObjIdNew =
                        dataObjectDestList.find { it.code == oneConfigClassDescr.code && it.id == oneLoadObjIdOld }!!.idDest

                    // Класс ссылочного поля типа InChild
                    val oneConfClassRefInChild =
                        jsonConfigFile.objects.find { it.code == oneRefObjInChildDescr.codeRef }!!

                    // Текущее значение ссылочного поля типа InChild в базе.
                    // Если refFieldValueOld = null, то в файле ссылочное поле не заполнено
                    val refFieldValueOld =
                        dataObjectDestList.find { it.code == oneConfigClassDescr.code && it.id == oneLoadObjIdOld }!!.listChildRef.find { it.fieldName == oneRefObjInChildDescr.refField }?.fieldValue

                    // Новое значение id объекта из поля типа InChild. Если refFieldValueNew = null, значит объект уже был в базе и значение ссылочного поля в бд правильное
                    val refFieldValueNew =
                        dataObjectDestList.find { it.code == oneConfClassRefInChild.code && it.id == refFieldValueOld }?.idDest

                    if (refFieldValueNew != null && refFieldValueOld != null && refFieldValueOld != refFieldValueNew) {

                        val indexOfRefField =
                            oneLoadObject.row.fields.indexOfFirst { it.fieldName == oneRefObjInChildDescr.refField }
                        oneLoadObject.row.fields[indexOfRefField].fieldValue = refFieldValueNew

                        val nextValueRevChildName = "nextValueChildId"

                        // Объявление переменных для значений сиквенса в psql
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

                        // Инициализация переменных для сиквенса в psql
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

                        val conn = DatabaseConnection.getConnection(javaClass.toString(), oneConfigClassDescr.aliasDb)
                        conn.autoCommit = false
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
                        conn.commit()
                        queryStatement.close()
                    }
                }
            }
        }
    }

    // Знаю ид тарифа в БД приемнике. Ищу связанную с ним шкалу
    public fun findScaleIdInDB(
        oneConfClassObj: ObjectCfg,
        idObjectInDB: String
    ): String {

        var idScaleInDB = ""

        // Если закачиваемый тариф есть в БД приемнике, то беру ид шкалы у него
        val connCompare = DatabaseConnection.getConnection(javaClass.toString(), oneConfClassObj.aliasDb)
        val queryStatement =
            connCompare.prepareStatement("select ${scalable.getScaleIdFieldName(oneConfClassObj)} from ${oneConfClassObj.tableName} where ${oneConfClassObj.keyFieldIn}=$idObjectInDB")

        val queryResult = queryStatement.executeQuery()

        while (queryResult.next()) {
            if (queryResult.getString(1) != null) {
                idScaleInDB = queryResult.getString(1)
            }
        }
        queryStatement.close()
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

    // Поиск в файле у загружаемого объекта референса типа keyType=InChild
    private fun getInChildIdForInParentObject(
        objFieldsList: List<Fields>,
        oneConfigClass: ObjectCfg,
        refFieldIdValue: String,
        nestedLevel: Int
    ): List<Fields> {
        val listChildRef = mutableListOf<Fields>()
        if (oneConfigClass.refObjects.find { it.keyType.lowercase() == "inchild" } != null && nestedLevel < 2) {
            for (oneRefObjInChildDescr in oneConfigClass.refObjects.filter { it.keyType.lowercase() == "inchild" }) {
                if (objFieldsList.find { it.fieldName == oneRefObjInChildDescr.refField }!!.fieldValue != null) {

                    // Для объекта, у которого есть референс с keyType=InChild,
                    //   запоминаю идентификатор этого референса из файла в списке listChildRef
                    // Например, для объекта classifierType будет запомнен идентификатор его референса classifierValue в следующем виде:
                    //   DataObjectDest(code=classifierType, id=100009, idDest=100582, listChildRef=[Fields(fieldName=default_value_id, fieldValue=100018)])
                    listChildRef.add(
                        Fields(
                            oneRefObjInChildDescr.refField,
                            objFieldsList.find { it.fieldName == oneRefObjInChildDescr.refField }!!.fieldValue!!
                        )
                    )

                    // Ищу объект, у которого есть референс с keyType=InChild, среди главных объектов.
                    // Заполняю поле главного объекта, в котором хранится ссылка на референс, значением null
                    // Например, для classifierType поле default_value_id станет равно null
                    for (item in checkObject.allCheckObject.element.filter {
                        it.code.equals(
                            oneConfigClass.code,
                            true
                        )
                    }) {
                        if (item.row.fields.find {
                                it.fieldName.equals(oneConfigClass.keyFieldIn, true)
                                        && it.fieldValue.equals(refFieldIdValue, true)
                            } != null) {
                            val indexOfRefField =
                                item.row.fields.indexOfFirst { it.fieldName == oneRefObjInChildDescr.refField }
                            item.row.fields[indexOfRefField].fieldValue = null
                        }
                    }
                }
            }
        }
        return listChildRef
    }

}