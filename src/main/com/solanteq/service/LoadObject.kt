package com.solanteq.service

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import kotlin.system.exitProcess

// класс, в котором связан id объекта из базы источника с id объекта в базе приемнике
data class DataObjectDest(
    val code: String,  // название класса из конфигурации
    val id: String,         // значение поля refFieldId из базы источника
    val idDest: String,      // значение поля refFieldId из базы приемника
    val listChildRef: List<Fields> // значение референсных полей типа InChild
)

class LoadObject {

    private var dataObjectDestList = mutableListOf<DataObjectDest>()

    private val logger = LoggerFactory.getLogger(javaClass)

    // коннект к БД
    private lateinit var conn: Connection

    //fun loadDataObject(allLoadObject: DataDBMain, jsonConfigFile: RootCfg) {
    fun loadDataObject() {

        // считывание конфигурации
        val readJsonFile = ReadJsonFile()
        val jsonConfigFile = readJsonFile.readConfig()
        logger.info("Configuration info: " + jsonConfigFile.cfgList[0])

        // формирования списка полей, которые не нужно выгружать, для каждого класса
        CommonFunctions().createListFieldsNotExport(jsonConfigFile)

        // считывание файла объектов
        val allLoadObject = readJsonFile.readObject()

        // проверка версии и названия конфигурационного файла и файла с объектами
        if (jsonConfigFile.cfgList[0].version != allLoadObject.cfgList[0].version ||
            jsonConfigFile.cfgList[0].cfgName != allLoadObject.cfgList[0].cfgName
        ) {
            logger.error("Different versions or cfgName of Configuration File and Object File.")
            exitProcess(-1)
        }

        // установка соединения с БД
        conn = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
        logger.info("DataBase connection parameters: dbconn=$CONN_STRING dbuser=$CONN_LOGIN dbpass=$CONN_PASS.")

        // цикл по главным объектам (главные объекты это объекты, которые нужно загрузить в БД).
        // если главный объект успешно загружен, то добавляю в список его полей Fields("@isLoad","1")
        for (oneLoadObject in allLoadObject.element) {

            // поиск описания класса референсного объекта в файле конфигурации
            val oneConfLoadObj = jsonConfigFile.objects.find { it.code == oneLoadObject.code }!!
            val chainLoadObject = mutableListOf<DataDB>(oneLoadObject)
            loadOneObject(chainLoadObject, oneConfLoadObj, jsonConfigFile, allLoadObject.element)
        }
        createQueryForFamilyObject(allLoadObject, jsonConfigFile)
        conn.close()

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
                CheckObject().findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)

            // нашли референсный объект среди главных объектов. теперь проверка референсов найденного главного объекта
            if (indexInAllLoadObject > -1) {

                // референсы подобного типа пропускаю, т.к. они заведомо закольцованы и при загрузке обрабатываются отдельным образом
                if (/*oneRefObject.typeRef.lowercase() == "inparent" ||*/ oneRefObject.typeRef.lowercase() == "inchild") {
                    continue
                }

                chainLoadObject.add(allLoadObject[indexInAllLoadObject])

                // рекурсивный поиск референсных объектов, которые есть среди главных объектов и их загрузка
                loadOneObject(chainLoadObject, oneConfClassRefObj, jsonConfigFile, allLoadObject)
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
                    CheckObject().findRefObjAmongMainObj(oneRefObject, oneConfClassRefObj, allLoadObject)

                // нашли референсный объект среди главных объектов. теперь проверка референсов найденного главного объекта
                if (indexInAllLoadObject > -1) {

                    chainLoadObject.add(allLoadObject[indexInAllLoadObject])

                    // рекурсивный поиск референсных объектов, которые есть среди главных объектов и их загрузка
                    loadOneObject(chainLoadObject, oneConfClassRefObj, jsonConfigFile, allLoadObject)
                }
            }
        }

        /*// установка нового значения ссылочного поля в файле для референса типа fieldJson
        setNewIdFieldJson(oneLoadObject.row.fields, oneLoadObject.row.refObject, oneConfClassObj, jsonConfigFile)
        for (oneLinkObject in oneLoadObject.row.linkObjects) {
            val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneLinkObject.code }!!
            setNewIdFieldJson(oneLinkObject.row.fields, oneLinkObject.row.refObject, oneConfClassRefObj, jsonConfigFile)
        }

        // установка нового значения ссылочного поля в файле для референса типа refFields
        setNewIdRefFields(oneLoadObject.row.fields, oneLoadObject.row.refObject, oneConfClassObj, jsonConfigFile, null)
        for (oneLinkObject in oneLoadObject.row.linkObjects) {
            val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneLinkObject.code }!!
            setNewIdRefFields(
                oneLinkObject.row.fields,
                oneLinkObject.row.refObject,
                oneConfClassRefObj,
                jsonConfigFile,
                oneLoadObject
            )
        }*/

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

        logger.debug(
            CommonFunctions().createObjectIdForLogMsg(
                oneConfClassObj.code,
                oneConfClassObj.keyFieldOut,
                oneLoadObject.row.fields,
                -1
            ) + "The object is loaded into the database. "
        )
        oneLoadObject.row.fields += Fields("@isLoad", "1")
    }

    // формирование запроса для изменения данных в базе
    private fun createSaveObjSql(
        oneLoadObject: DataDB,
        oneConfClassObj: ObjectCfg,
        allLoadObject: List<DataDB>,
        jsonConfigFile: RootCfg
    ) {

        val sqlQueryUpdate: String
        val sqlQueryInsertAud: String
        val sqlQueryRefTables: String
        val sqlQueryInsert: String
        val sqlQuery: String
        val nextValueRevName = "nextValueRev"

        // получаю идентификатор объекта в базе приемнике, либо генерю новый
        val idObjectInDB = getObjIdInDB(oneLoadObject.row.fields, oneConfClassObj, 0, true)

        // установка нового значения ссылочного поля в файле для референса типа fieldJson
        setNewIdFieldJson(oneLoadObject.row.fields, oneLoadObject.row.refObject, oneConfClassObj, jsonConfigFile)
        for (oneLinkObject in oneLoadObject.row.linkObjects) {
            val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneLinkObject.code }!!
            setNewIdFieldJson(oneLinkObject.row.fields, oneLinkObject.row.refObject, oneConfClassRefObj, jsonConfigFile)
        }

        // установка нового значения ссылочного поля в файле для референса типа refFields
        setNewIdRefFields(oneLoadObject.row.fields, oneLoadObject.row.refObject, oneConfClassObj, jsonConfigFile, null)
        for (oneLinkObject in oneLoadObject.row.linkObjects) {
            val oneConfClassRefObj = jsonConfigFile.objects.find { it.code == oneLinkObject.code }!!
            setNewIdRefFields(
                oneLinkObject.row.fields,
                oneLinkObject.row.refObject,
                oneConfClassRefObj,
                jsonConfigFile,
                oneLoadObject
            )
        }

        // формирование условий запроса update/insert
        sqlQueryUpdate = createMainInsUpdQuery(oneLoadObject, oneConfClassObj, idObjectInDB, "", "update")
        sqlQueryInsert = createMainInsUpdQuery(oneLoadObject, oneConfClassObj, idObjectInDB, "", "insert")

        // формирование условий запроса insert в таблицу аудита
        sqlQueryInsertAud =
                //createAudInsQuery(oneConfClassObj, idObjectInDB, nextValueRevName)
            createMainInsUpdQuery(oneLoadObject, oneConfClassObj, idObjectInDB, nextValueRevName, "insertAudValues")

        // формирование условий запроса для таблицы связей (референсы refTables)
        sqlQueryRefTables =
            createRefTablesQuery(
                oneLoadObject,
                oneConfClassObj,
                allLoadObject,
                jsonConfigFile,
                idObjectInDB,
                "mainObject"
            )

        // запрос на изменение объектов linkObjects
        val (sqlQueryLinkObjDeclare, sqlQueryLinkObjInit, sqlQueryLinkObject) =
            createLinkObjectsQuery(
                oneLoadObject,
                oneConfClassObj,
                jsonConfigFile,
                idObjectInDB
            )

        // запрос на изменение основного объекта, его refTables и linkObjects
        sqlQuery = "\nDO $$ \n" +
                "declare $nextValueRevName integer; \n" +
                "declare revTimeStamp numeric(18,0); \n" +
                "declare dateNow timestamp; \n" +
                sqlQueryLinkObjDeclare +
                "BEGIN \n" +
                "$nextValueRevName = nextval('${CommonConstants().commonSequence}'); \n" +
                "dateNow = now(); \n" +
                "revTimeStamp = cast(extract(epoch from dateNow) * 1000 as numeric(18,0)); \n" +
                sqlQueryLinkObjInit +
                "if exists(select 1 from ${oneConfClassObj.tableName} where ${oneConfClassObj.keyFieldIn}='$idObjectInDB') then \n" +
                sqlQueryUpdate +
                "else \n" +
                sqlQueryInsert +
                "end if; \n" +
                sqlQueryInsertAud +
                sqlQueryRefTables +
                sqlQueryLinkObject +
                "END $$; \n"

        createTranSaveObj(sqlQuery, oneLoadObject, oneConfClassObj)
    }

    // внести изменение в базу в рамках транзакции
    private fun createTranSaveObj(
        sqlQuery: String,
        oneLoadObject: DataDB,
        oneConfClassObj: ObjectCfg
    ) {

        try {
            val connect = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
            connect.autoCommit = false
            val queryStatement = connect.prepareStatement(sqlQuery)
            logger.debug(
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
            connect.close()
        } catch (e: Exception) {
            logger.error(
                CommonFunctions().createObjectIdForLogMsg(
                    oneConfClassObj.code,
                    oneConfClassObj.keyFieldOut,
                    oneLoadObject.row.fields,
                    -1
                ) + "\n" + "<The error with query>. " + e.message
            )
            exitProcess(-1)
        }
    }

    // формирование запроса на вставку данных в таблицы связи, описанные в параметре refTables файла конфигурации
    private fun createRefTablesQuery(
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
                        CheckObject().findRefObjAmongMainObj(oneRefTables, oneRefLinkClass, allLoadObject)
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

    // формирование условий запроса для референсов linkObjects
    private fun createLinkObjectsQuery(
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
                        createMainInsUpdQuery(
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

            var filterObjCond = ""
            if (oneLinkObjClass.filterObjects != "") {
                filterObjCond += " and ${oneLinkObjClass.filterObjects} "
            }

            val sqlQuery = "select * " +
                    "from ${oneLinkObjClass.tableName} " +
                    "where audit_state = 'A' and ${oneLinkObjDescription.refField} = $idObjectInDB $filterObjCond"
            logger.debug(
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
                        if (fieldName != oneLinkObjClass.keyFieldIn && oneLinkObject.row.fields.find { it.fieldName == fieldName }!!.fieldValue != fieldValue) {
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

                        if (CheckObject().compareObjectRefTables(
                                linkObjectFromDBId,
                                oneLinkObjClass,
                                oneLinkObject.row.refObject,
                                oneLinkObject.row.fields,
                                jsonConfigFile
                            )
                        ) {
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
                            break
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

                    sqlQueryLinkObject += "\n update ${oneLinkObjClass.tableName} set valid_to=CURRENT_DATE-1 where ${oneLinkObjClass.keyFieldIn}=$linkObjectFromDBId; \n"

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
                        createMainInsUpdQuery(
                            null,
                            oneLinkObjClass,
                            linkObjectFromDBId,
                            nextValueRevLinkName,
                            "insertAudSelectValue"
                        )
                }
            }
            queryResult.close()

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

                val idLinkObjectInDB = nextSequenceValue(oneLinkObjClass.sequence)

                // insert в таблицу объекта linkObject
                sqlQueryLinkObject += "\n " + createMainInsUpdQuery(
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
                    createMainInsUpdQuery(
                        oneLinkObject,
                        oneLinkObjClass,
                        idLinkObjectInDB,
                        nextValueRevLinkName,
                        "insertAudValues"
                    )

                // insert в таблицу связей refObject для объекта linkObject
                sqlQueryLinkObject +=
                    "\n" + createRefTablesQuery(
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

    // формирование запроса insert/update для основной таблицы загружаемого объекта
    private fun createMainInsUpdQuery(
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

        // формирование условий запроса update/insert
        if (oneLoadObject != null) {

            for (field in oneLoadObject.row.fields) {
                if (oneConfClassObj.fieldsNotExport.find { it.name == field.fieldName } == null &&
                    field.fieldName != oneConfClassObj.keyFieldIn) {
                    listColName += field.fieldName + ","
                    if (field.fieldValue == null) {
                        listColNameColValue += "${field.fieldName}=${field.fieldValue}, "
                        listColValue += "${field.fieldValue},"
                    } else {
                        listColNameColValue += "${field.fieldName}='${field.fieldValue}', "
                        listColValue += "'${field.fieldValue}',"
                    }
                }
            }
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

        } else {

            sqlQuery = "update ${oneConfClassObj.tableName} set $listColNameColValue " +
                    "where ${oneConfClassObj.keyFieldIn}='$idObjectInDB'; \n"

        }
        return sqlQuery
    }

    // получаю идентификатор объекта в базе приемнике, либо генерю новый
    fun getObjIdInDB(
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

        val checkObject = CheckObject()

        // поиск объекта в БД
        idObjectInDB = checkObject.findObjectInDB(objFieldsList, oneConfigClass, -1)

        // генерация нового значения id, если объект не был найден в бд
        if (idObjectInDB == "-" && isGenerateNewSeqValue) {
            idObjectInDB = nextSequenceValue(oneConfigClass.sequence)
        }

        // добавляю объект в список уже найденных в бд приемнике
        if (idObjectInDB != "-" &&
            //!dataObjectDestList.contains(DataObjectDest(oneConfigClass.code, refFieldIdValue, idObjectInDB))
            dataObjectDestList.find { it.code == oneConfigClass.code && it.id == refFieldIdValue && it.idDest == idObjectInDB } == null
        ) {
            val listChildRef = mutableListOf<Fields>()
            if (oneConfigClass.refObjects.find { it.keyType.lowercase() == "inchild" } != null && nestedLevel < 2) {
                for (oneRefObjInChildDescr in oneConfigClass.refObjects.filter { it.keyType.lowercase() == "inchild" }) {
                    listChildRef.add(
                        Fields(
                            oneRefObjInChildDescr.refField,
                            objFieldsList.find { it.fieldName == oneRefObjInChildDescr.refField }!!.fieldValue!!
                        )
                    )
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
    private fun nextSequenceValue(sequence: String): String {

        val sqlQuery = "SELECT nextval('${sequence}')"
        val connNextSequence = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)
        val queryStatement = connNextSequence.prepareStatement(sqlQuery)
        val queryResult = queryStatement.executeQuery()

        queryResult.next()
        val nextValue = queryResult.getString(1)
        queryResult.close()
        connNextSequence.close()

        return nextValue
    }

    // установка нового значения ссылочного поля для референса типа fieldJson
    public fun setNewIdFieldJson(
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
                                        getObjIdInDB(
                                            oneRefObject.row.fields,
                                            oneRefFieldJsonClass,
                                            oneRefObject.nestedLevel,
                                            true
                                        )
                                    if (itemFromAttribute is ObjectNode) {
                                        (itemFromAttribute as ObjectNode).put(fieldNameInJsonStr, refFieldValueNew)
                                    } else if (attributeRecord is ObjectNode) {
                                        (attributeRecord as ObjectNode).put(fieldNameInJsonStr, refFieldValueNew)
                                    } else {
                                        (attributeRecord as ArrayNode).set(index, refFieldValueNew)
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

    // установка нового значения ссылочного поля в файле для референса типа refFields
    public fun setNewIdRefFields(
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
                idObjectInDB = getObjIdInDB(oneMainLoadObject.row.fields, oneMainLoadObjClass, 0, true)
            } else if (oneRefObject == null) {
                continue
            } else if (oneRefObject.typeRef.lowercase() == "inchild") {
                idObjectInDB = getObjIdInDB(oneRefObject.row.fields, oneRefClassObj, oneRefObject.nestedLevel, false)
            } else {
                // поиск id референсного объекта в бд приемнике
                idObjectInDB = getObjIdInDB(oneRefObject.row.fields, oneRefClassObj, oneRefObject.nestedLevel, true)
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

    // создание запроса для изменения полей реверенсов типа inchild
    private fun createQueryForFamilyObject(
        allLoadObject: DataDBMain,
        jsonConfigFile: RootCfg
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
                    val refFieldValueOld =
                        dataObjectDestList.find { it.code == oneConfigClassDescr.code && it.id == oneLoadObjIdOld }!!.listChildRef.find { it.fieldName == oneRefObjInChildDescr.refField }!!.fieldValue
                    //oneLoadObject.row.fields.find { it.fieldName == oneRefObjInChildDescr.refField }!!.fieldValue

                    // новое значение id объекта из поля типа inchild. если refFieldValueNew = null, значит объект уже был в базе и значение ссылочного поля в бд правильное
                    val refFieldValueNew =
                        dataObjectDestList.find { it.code == oneConfClassRefInChild.code && it.id == refFieldValueOld }?.idDest

                    if (refFieldValueNew != null && refFieldValueOld != refFieldValueNew) {
                        val sqlQuery =
                            "update ${oneConfigClassDescr.tableName} set ${oneRefObjInChildDescr.refField}=$refFieldValueNew " +
                                    " where ${oneConfigClassDescr.keyFieldIn}=$oneLoadObjIdNew"
                        val queryStatement = conn.prepareStatement(sqlQuery)
                        logger.debug(
                            CommonFunctions().createObjectIdForLogMsg(
                                oneConfigClassDescr.code,
                                oneConfigClassDescr.keyFieldOut,
                                oneLoadObject.row.fields,
                                -1
                            ) + "Query to update reference field with <keyType=InChild> field ${oneRefObjInChildDescr.refField}: $sqlQuery"
                        )
                        queryStatement.executeUpdate()
                    }
                }
            }
        }
    }
}