package com.solanteq.service

import com.solanteq.security.PasswordEncryptor
import org.slf4j.LoggerFactory
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

class CommonConstants {

    // версия программы
    val VERSION = "1.0.5.15.1"

    // уровень вложенности рекурсии при чтении ссылочных объектов
    val NESTED_LEVEL_REFERENCE = 2

    // массив полей, которые не выгружаются из БД
    val FIELDS_NOT_EXPORT = arrayOf(
        "audit_date",
        "audit_state",
        "audit_user_id",
        "scale_id",
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
    logger.info("SOLAR DataBase Copy Configuration Tool " + CommonConstants().VERSION)

    var argsString = " "
    for (arg in args) {
        argsString += "$arg "
    }
    logger.info("Argument string: $argsString")

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
            val checkObject = CheckObject()
            logger.info("Start of object file verification")
            checkObject.checkDataObject()
            logger.info("The object file has been successfully verified")
        }

        // режим загрузки файла объектоа
        if (REGIM == CommonConstants().REGIM_LOAD_OBJFILE) {
            val checkObject = CheckObject()
            val loadObject = LoadObject()
            logger.info("Start of object file verification")
            checkObject.checkDataObject()
            logger.info("The object file has been successfully verified")
            logger.info("Start load object file")
            //loadObject.loadDataObject(checkObject.allCheckObject, checkObject.jsonConfigFile)
            loadObject.loadDataObject()
            logger.info("The object file has been uploaded successfully")
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
