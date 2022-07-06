package com.solanteq.service

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import ch.qos.logback.classic.spi.LoggerContextListener
import ch.qos.logback.core.joran.spi.JoranException
import com.solanteq.security.PasswordEncryptor
import org.slf4j.LoggerFactory
import java.io.File
import java.sql.DriverManager
import kotlin.system.exitProcess

public var REGIM = ""
public var CONN_STRING = ""
public var CONN_LOGIN = ""
private var CONN_PASSOPEN = ""
private var CONN_PASSENCRYPT = ""
public var CONN_PASS = ""
public var CONFIG_FILE = ""
public var TASK_FILE = ""
public var OBJECT_FILE = ""
public var FILTER_FILE = ""
public var AUDIT_DATE = ""
//private val LOGBACK_FILE_PATH = "config\\logback.xml"
private val LOGBACK_FILE_PATH = "config/logback.xml"

// Класс описывает действия с объектом при проверке/загрузке
data class ActionWithObject(
    var id: String,
    var classCode: String,
    var actionInsert: Boolean,
    var actionUpdateMainRecord: Boolean,
    var actionUpdateRefTables: Boolean,
    var actionUpdateLinkRecord: Boolean,
    // При проверке(check), если actionUpdateLinkRecord = true, то заполняются поля с запросами.
    // Поэтому при загрузке(load) не нужно повторно формировать запросы
    var queryToUpdateLinkRecDeclare: String,
    var queryToUpdateLinkRecInit: String,
    var queryToUpdateLinkRecObject: String,
    ///////////////////////////////////////////
    var actionSkip: Boolean
)

class CommonConstants {

    // версия программы
    val VERSION = "1.0.5.19"

    // уровень вложенности рекурсии при чтении ссылочных объектов
    val NESTED_LEVEL_REFERENCE = 2

    //val SCALE_ID_FIELD_NAME = "scale_id"
    //val SCALE_COMPONENT_ID_FIELD_NAME = "scale_component_id"
    //val TARIFF_CLASS_NAME = "tariff"
    //val TARIFF_VALUE_CLASS_NAME = "tariffvalue"
    //val NUMBER_TARIFF_VALUE_CLASS_NAME = "numbertariffvalue"
    //val SCALE_COMPONENT_CLASS_NAME = "scalecomponent"
    //val SCALE_COMPONENT_VALUE_CLASS_NAME = "scalecomponentvalue"
    //val SCALE_AMOUNT_CLASS_NAME = "scalableamount"
    //val SCALE_CLASS_NAME = "scale"

    // массив полей, которые не выгружаются из БД
    val FIELDS_NOT_EXPORT = arrayOf(
        "audit_date",
        "audit_state",
        "audit_user_id",
        //"scale_id",
        "@isLoad"   // это не колонка из таблицы, это поле используется для проверки загружен объект в базу или нет
    )

    // допустимые ключи
    public val ALLOWED_COMMANDS = arrayOf(
        "-m",   // Режим запуска
        "-c",   // Строка подключения к базе данных
        "-url",  // Строка подключения к базе данных
        "-username",   // Логин
        "-password",   // Пароль в открытом виде
        "-encryptedPassword",   // Пароль в зашифрованном виде
        "-s",   // Имя файла структуры объектов
        "-w",   // Имя файла задания
        "-r",   // Имя файла результата
        "-f",   // Имя файла фильтра
        "-d"    // Дата в формате YYYY-MM-DD HH:MM:SS
    )

    public val commonSequence = "seq_revinfo" // общий сиквенс для всей системы

    public val REGIM_CREATE_OBJFILE = "save"
    public val REGIM_CREATE_TASKFILE = "dc"
    public val REGIM_CHECK_OBJFILE = "check"
    public val REGIM_LOAD_OBJFILE = "load"

}

fun main(args: Array<String>) {

    val logger = LoggerFactory.getLogger("Main")

    if (File(LOGBACK_FILE_PATH).exists()) {
        //logger.error("Not found logback.xml file on the path <$LOGBACK_FILE_PATH>")
        //exitProcess(-1)

        val context = LoggerFactory.getILoggerFactory() as LoggerContext
        try {
            val configurator = JoranConfigurator()
            configurator.context = context;
            //context.reset();
            configurator.doConfigure(LOGBACK_FILE_PATH)
        } catch (e: JoranException) {
            println("JoranException : $e")
            exitProcess(-1)
        }
        //StatusPrinter.printInCaseOfErrorsOrWarnings(context);
    }

    //val logger = LoggerFactory.getLogger("Main")
    logger.info("SOLAR DataBase Copy Configuration Tool " + CommonConstants().VERSION)

    var argsString = " "
    for (arg in args) {
        argsString += "$arg "
    }
    //logger.info("Argument string: $argsString")

    val dbUpdateService = DBUpdateService()
    dbUpdateService.setInputArgs(args)
    dbUpdateService.checkInputArgs(args)

    if (REGIM == CommonConstants().REGIM_CREATE_OBJFILE || REGIM == CommonConstants().REGIM_CREATE_TASKFILE ||
        REGIM == CommonConstants().REGIM_CHECK_OBJFILE || REGIM == CommonConstants().REGIM_LOAD_OBJFILE
    ) {

        // режим создания файла задания
        if (REGIM == CommonConstants().REGIM_CREATE_TASKFILE) {
            val readerDB = ReaderDB()
            val writerJson = WriterJson()
            logger.info("Start creating the task file")
            readerDB.readAllObject()
            writerJson.createJsonFile(readerDB.taskFileFieldsMain, TASK_FILE)
            logger.info("The task file has been created successfully")
        }

        // режим выгрузки объектов по файлу задания
        if (REGIM == CommonConstants().REGIM_CREATE_OBJFILE) {
            val readerDB = ReaderDB()
            val writerJson = WriterJson()
            logger.info("Start creating the object file")
            readerDB.readTaskObjects()
            writerJson.createJsonFile(readerDB.tblMainMain, OBJECT_FILE)
            logger.info("The object file has been created successfully")
        }

        // режим проверки файла объектов
        if (REGIM == CommonConstants().REGIM_CHECK_OBJFILE) {

            // считывание файла объектов
            val readJsonFile = ReadJsonFile()
            val allDataObject = readJsonFile.readObject()

            // действие при загрузке для каждого объекта из файла: добавление/обновление/пропуск
            val listOfActionWithObject = mutableListOf<ActionWithObject>()

            val checkObject = CheckObject
            checkObject.allCheckObject = allDataObject
            checkObject.listOfActionWithObject = listOfActionWithObject

            logger.info("Start of object file verification")
            try {
                checkObject.checkDataObject()
            } catch (e: Exception) {

                var stackTrace = ""
                for (item in e.stackTrace) {
                    stackTrace += "$item\n"
                }

                logger.error(
                    "Verification error: $e\n$stackTrace"
                )
                exitProcess(-1)
            }
            logger.info("The object file has been successfully verified")
        }

        // режим загрузки файла объектов
        if (REGIM == CommonConstants().REGIM_LOAD_OBJFILE) {
            // считывание файла объектов
            val readJsonFile = ReadJsonFile()
            val allDataObject = readJsonFile.readObject()

            // действие при загрузке для каждого объекта из файла: добавление/обновление/пропуск
            val listOfActionWithObject = mutableListOf<ActionWithObject>()

            // коннект к БД
            //val conn = DriverManager.getConnection(CONN_STRING, CONN_LOGIN, CONN_PASS)

            val checkObject = CheckObject
            checkObject.allCheckObject = allDataObject
            checkObject.listOfActionWithObject = listOfActionWithObject

            //val loadObject = LoadObject(allDataObject)
            val loadObject = LoadObject
            loadObject.allLoadObject = allDataObject
            loadObject.listOfActionWithObject = listOfActionWithObject
            //loadObject.conn = conn

            logger.info("Start of object file verification")
            try {
                checkObject.checkDataObject()
            } catch (e: Exception) {

                var stackTrace = ""
                for (item in e.stackTrace) {
                    stackTrace += "$item\n"
                }
                logger.error(
                    "Verification error: $e\n$stackTrace"
                )
                exitProcess(-1)

            }
            logger.info("The object file has been successfully verified\n")

            logger.info("Start load object file")
            try {
                loadObject.loadDataObject()
            } catch (e: Exception) {

                var stackTrace = ""
                for (item in e.stackTrace) {
                    stackTrace += "$item\n"
                }
                logger.error(
                    "Loading error: $e\n$stackTrace"
                )
                exitProcess(-1)

            }
            logger.info("The object file has been successfully uploaded")

            //conn.close()
        }

    } else {
        logger.error("Unknown startup mode")
    }
}


class DBUpdateService {

//companion object {
///JvmStatic

    private val logger = LoggerFactory.getLogger("DBUpdateService")

    // запись аргументов командной строки в глобальные переменные
    fun setInputArgs(args: Array<String>) {
        for (arg in args) {
            when (arg) {
                "-m" -> REGIM = args[args.indexOf(arg) + 1]
                /*"-c" -> {
                    val arrConn = args[args.indexOf(arg) + 1].split(" ")
                    CONN_STRING = arrConn[0]
                    if (arrConn.size > 1) {
                        CONN_LOGIN = arrConn[1]
                    }
                    if (arrConn.size > 2) {
                        CONN_PASS = arrConn[2]

                        //println(PasswordEncryptor.decrypt("5ghJMCUNnlCwLq4/mY9blw==;/4lq1mJbMTYQOjiYY9qziw=="))
                    }
                }*/
                "-url" -> CONN_STRING = args[args.indexOf(arg) + 1]
                "-username" -> CONN_LOGIN = args[args.indexOf(arg) + 1]
                "-password" -> {
                    CONN_PASSOPEN = args[args.indexOf(arg) + 1]
                    val password: String
                    if (CONN_PASSOPEN == "PROMT") {
                        print("Enter password: ")
                        password = System.console()?.readPassword()?.joinToString("") ?: (readLine() ?: "")
                    } else {
                        password = CONN_PASSOPEN
                    }
                    CONN_PASS = password
                }
                "-encryptedPassword" -> {
                    CONN_PASSENCRYPT = args[args.indexOf(arg) + 1]
                    val encryptedPass: String
                    if (CONN_PASSENCRYPT == "PROMT") {
                        print("Enter password: ")
                        encryptedPass = System.console()?.readPassword()?.joinToString("") ?: (readLine() ?: "")
                    } else {
                        encryptedPass = CONN_PASSENCRYPT
                    }

                    try {
                        CONN_PASS = PasswordEncryptor.decrypt(encryptedPass)
                    } catch (e: Exception) {
                        logger.error(e.message)
                        exitProcess(-1)
                    }
                }
                "-s" -> CONFIG_FILE = args[args.indexOf(arg) + 1].lowercase()
                "-w" -> TASK_FILE = args[args.indexOf(arg) + 1].lowercase()
                "-r" -> OBJECT_FILE = args[args.indexOf(arg) + 1].lowercase()
                "-f" -> FILTER_FILE = args[args.indexOf(arg) + 1].lowercase()
                "-d" -> AUDIT_DATE = args[args.indexOf(arg) + 1]
            }
        }
    }

    // проверка заполнения аргументов командной строки
    fun checkInputArgs(args: Array<String>) {

        for (arg in args) {
            if (arg.indexOf("-") == 0 && !CommonConstants().ALLOWED_COMMANDS.contains(arg)) {
                logger.error("<Wrong input key>: The key <$arg> doesn't supported")
                exitProcess(-1)
            }
        }

        if (REGIM == "") {
            logger.error("<The regim is not specified>")
            exitProcess(-1)
        }
        if (CONN_STRING == "") {
            logger.error("<The Connection String must be specified>")
            exitProcess(-1)
        }
        if (CONN_LOGIN == "") {
            logger.error("<The UserLogin must be specified in the Connection String>")
            exitProcess(-1)
        }
        if (CONN_PASSOPEN == "" && CONN_PASSENCRYPT == "") {
            logger.error("<The UserPassword or UserPasswordEncrypt must be specified in the Connection String>")
            exitProcess(-1)
        }
        if (CONN_PASSOPEN != "" && CONN_PASSENCRYPT != "") {
            logger.error("<Only one of the UserPassword and UserPasswordEncrypt must be specified in the Connection String>")
            exitProcess(-1)
        }
        if (CONFIG_FILE == "") {
            logger.error("<The Config File must be specified>")
            exitProcess(-1)
        }

        if (REGIM == CommonConstants().REGIM_CREATE_OBJFILE && (TASK_FILE == "" || OBJECT_FILE == "")) {
            logger.error("<For the <${CommonConstants().REGIM_CREATE_OBJFILE}> Regim must be specified the Task File and Result File>")
            exitProcess(-1)
        }

        if (REGIM == CommonConstants().REGIM_CREATE_TASKFILE && (AUDIT_DATE == "" || TASK_FILE == "")) {
            logger.error("<For the <${CommonConstants().REGIM_CREATE_TASKFILE}> Regim must be specified the Audit Date and Task File>")
            exitProcess(-1)
        }

        if (REGIM == CommonConstants().REGIM_LOAD_OBJFILE && (OBJECT_FILE == "")) {
            logger.error("<For the <${CommonConstants().REGIM_LOAD_OBJFILE}> Regim must be specified the Object File>")
            exitProcess(-1)
        }

        if (REGIM == CommonConstants().REGIM_LOAD_OBJFILE && (OBJECT_FILE == "")) {
            logger.error("<For the <${CommonConstants().REGIM_LOAD_OBJFILE}> Regim must be specified the Object File>")
            exitProcess(-1)
        }

    }
//}
}
