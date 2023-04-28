package com.solanteq.service

import com.solanteq.security.PasswordEncryptor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import kotlin.system.exitProcess

class DatabaseConnection() {

    companion object {

        //@JvmStatic

        // создание подключений к базам данных и их запись в глобальную переменную.
        // если нет файла с источниками подключений, то подключение будет только одно.
        fun createConnections(className: String) {

            val logger = LoggerFactory.getLogger(className)

            if (DATASOURCES_FILE != "") {
                for (source in dataSourcesSettings.dataSources) {

                    var password: String

                    if (source.password == null && source.encryptedPassword == null) {
                        logger.error(
                            "The aliasDB <${source.aliasDb}> in the DataSources file is filled in incorrectly. " +
                                    "The password and encryptedPassword are not filled."
                        )
                        exitProcess(-1)
                    }

                    if (source.encryptedPassword != null && (source.pswComponent1 == null || source.pswComponent2 == null)) {
                        logger.error(
                            "The aliasDB <${source.aliasDb}> in the DataSources file is filled in incorrectly. " +
                                    "The password decryption components are not filled."
                        )
                        exitProcess(-1)
                    }

                    // если не указан явный пароль, то работаю с зашифрованным.
                    if (source.password == null) {

                        val encryptedPass: String

                        // зашифрованный пароль будет введен с клавиатуры
                        if (source.encryptedPassword == "PROMT") {
                            print("Enter encrypted password for aliasDb <${source.aliasDb}> and username <${source.username}>: ")
                            encryptedPass = System.console()?.readPassword()?.joinToString("") ?: (readLine() ?: "")
                        } else {
                            encryptedPass = source.encryptedPassword!!
                        }

                        // расшифровка пароля
                        try {
                            System.setProperty("component1", source.pswComponent1!!)
                            System.setProperty("component2", source.pswComponent2!!)
                            password = PasswordEncryptor.decrypt(encryptedPass)
                        } catch (e: Exception) {
                            logger.error(
                                "The aliasDB <${source.aliasDb}> in the DataSources file is filled in incorrectly. " +
                                        "An error occurred while decrypting the password."
                            )
                            logger.error(e.message)
                            exitProcess(-1)
                        }
                    } else {
                        // явный пароль будет введен с клавиатуры
                        if (source.password == "PROMT") {
                            print("Enter password for aliasDb <${source.aliasDb}> and username <${source.username}>: ")
                            password = System.console()?.readPassword()?.joinToString("") ?: (readLine() ?: "")
                        } else {
                            password = source.password
                        }
                    }

                    dataSourceConnections.put(
                        source.aliasDb,
                        setConnection(className, source.jdbcUrl, source.username, password)
                    )

                }
            } else {
                dataSourceConnections.put(MAIN_ALIASDB, setConnection(className, CONN_STRING, CONN_LOGIN, CONN_PASS))
            }

        }

        // создание
        private fun setConnection(
            className: String,
            connString: String,
            connLogin: String,
            connPass: String
        ): Connection {

            val conn: Connection
            val logger = LoggerFactory.getLogger(className)

            try {
                conn = DriverManager.getConnection(connString, connLogin, connPass)
            } catch (e: Exception) {

                var stackTrace = ""
                for (item in e.stackTrace) {
                    stackTrace += "$item\n"
                }

                logger.error("Error connection to DataBase <$connString>: $e\n$stackTrace")
                exitProcess(-1)
            }

            return conn
        }

        fun getConnection(className: String, aliasDB: String?): Connection {

            val conn: Connection
            val logger = LoggerFactory.getLogger(className)
            var aliasDBLocal = aliasDB

            /*if (aliasDBLocal == null || DATASOURCES_FILE == "") {
                aliasDBLocal = MAIN_ALIASDB
            }*/
            aliasDBLocal = getAliasConnection(aliasDB)

            if (dataSourceConnections[aliasDBLocal] == null) {
                logger.error("Error when searching for connection with aliasDB <$aliasDBLocal>.")
                exitProcess(-1)
            } else {
                conn = dataSourceConnections[aliasDBLocal]!!
            }
            return conn
        }

        fun getAliasConnection(aliasDB: String?):String {
            var aliasDBLocal = aliasDB
            if (aliasDBLocal == null || DATASOURCES_FILE == "") {
                aliasDBLocal = MAIN_ALIASDB
            }
            return aliasDBLocal
        }

        // проверка того, что алиасы БД указанные в файле конфигурации есть в файле с источниками БД
        //   при этом в файле конфигурации нельзя указать пустой алиас: <"aliasDb": "">
        fun checkAliasDB(className: String) {
            val logger = LoggerFactory.getLogger(className)

            val readJsonFile = ReadJsonFile()
            val jsonConfigFile = readJsonFile.readConfig()

            if (DATASOURCES_FILE != "") {

                var listOfAliasDB: String = ""
                for (element in dataSourcesSettings.dataSources) {

                    if (element.aliasDb == "") {
                        logger.error("The DataSources file has empty aliasDb. It is necessary to fill in each alias.")
                        exitProcess(-1)
                    }

                    listOfAliasDB += ", ${element.aliasDb}"
                }

                for (element in jsonConfigFile.objects) {
                    if ((element.aliasDb == null) || (element.aliasDb == "") ||
                        (!listOfAliasDB.contains(element.aliasDb.toString(), ignoreCase = true))
                    ) {
                        logger.error(
                            "The aliasDB <${element.aliasDb}> for class <${element.code}> is filled in incorrectly. " +
                                    "There is no such aliasDB in DataSources file."
                        )
                        exitProcess(-1)
                    }
                }
            }
        }

        // проверка того, что объект и его linkObjects находятся в одной БД
        fun checkAliasDBForLinkObjects(className: String) {

            val readJsonFile = ReadJsonFile()
            val jsonConfigFile = readJsonFile.readConfig()
            val logger = LoggerFactory.getLogger(className)

            for (oneObjectClass in jsonConfigFile.objects) {
                checkAliasDBForOneLinkObject(className, oneObjectClass, jsonConfigFile, logger)
            }

        }

        // рекурсивная проверка того, что объект и его linkObjects находятся в одной БД
        private fun checkAliasDBForOneLinkObject(
            className: String,
            oneObjectClass: ObjectCfg,
            jsonConfigFile: RootCfg,
            logger: Logger
        ) {

            if (oneObjectClass.linkObjects.isNotEmpty()) {
                for (oneLinkObjectRef in oneObjectClass.linkObjects) {
                    val oneLinkObjectClass = jsonConfigFile.objects.find { it.code == oneLinkObjectRef.codeRef }!!
                    if (oneObjectClass.aliasDb != oneLinkObjectClass.aliasDb) {
                        logger.error(
                            "The aliasDB <${oneObjectClass.aliasDb}> of a class <${oneObjectClass.code}> is different " +
                                    "from the aliasDb <${oneLinkObjectClass.aliasDb}> of its linkObject class <${oneLinkObjectClass.code}>."
                        )
                        exitProcess(-1)
                    }
                    checkAliasDBForOneLinkObject(className, oneLinkObjectClass, jsonConfigFile, logger)
                }
            }
        }

//        // проверка того, что объект и его референсы refTables находятся в одной БД
//        fun checkAliasDBForRefTables(className: String) {
//
//            val readJsonFile = ReadJsonFile()
//            val jsonConfigFile = readJsonFile.readConfig()
//            val logger = LoggerFactory.getLogger(className)
//
//            for (oneObjectClass in jsonConfigFile.objects) {
//
//                if (oneObjectClass.refTables.isNotEmpty()) {
//                    for (oneRefTablesRef in oneObjectClass.refTables) {
//                        val oneRefTablesClass = jsonConfigFile.objects.find { it.code == oneRefTablesRef.codeRef }!!
//                        if (oneObjectClass.aliasDb != oneRefTablesClass.aliasDb) {
//                            logger.error(
//                                "The aliasDB <${oneObjectClass.aliasDb}> of a class <${oneObjectClass.code}> is different " +
//                                        "from the aliasDb <${oneRefTablesClass.aliasDb}> of its refTables class <${oneRefTablesClass.code}>."
//                            )
//                            exitProcess(-1)
//                        }
//                    }
//                }
//
//            }
//
//        }

    }

}

