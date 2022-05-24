package com.solanteq.service

import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

class Scalable(jsonConfigFile: RootCfg) {

    private val jsonConfigFileLocal = jsonConfigFile

    private val logger = LoggerFactory.getLogger(javaClass)

    // Название поля(scale_component_id) связывающее значение тарифа с компонентом шкалы тарифа
    var scaleComponentFieldName = ""

    // Название поля(component_id) связывающее компонент шкалы тарифа со значением компонента шкалы тарифа
    var scaleComponentValueFieldName = ""

    // Название поля(scalable_amount_id) связывающее значение компонента шкалы тарифа с измеряемой величиной тарифа
    var scalableAmountFieldName = ""

    // Название класса(scaleComponent) описывающего компонент шкалы тарифа
    var scaleComponentClassName = ""

    // Название класса(scaleComponentValue) описывающего значение компонента шкалы тарифа
    var scaleComponentValueClassName = ""

    // Название класса(scalableAmount) описывающего измеряемую величину тарифа
    var scalableAmountClassName = ""

    init {
        scaleComponentFieldName = this.getScaleFieldNameByKeyType("InScaleComponent")
        scaleComponentValueFieldName = this.getScaleFieldNameByKeyType("InScaleComponentValue")
        scalableAmountFieldName = this.getScaleFieldNameByKeyType("InScalableAmount")

        scaleComponentClassName = this.getScaleComponentConfigClassName()
        scaleComponentValueClassName = this.getScaleComponentValueConfigClassName()
        scalableAmountClassName = this.getScalableAmountConfigClassName()
    }

    public fun getClassDescriptionByCode(
        codeClass: String
    ): ObjectCfg {

        var configClass: ObjectCfg? = null

        configClass = jsonConfigFileLocal.objects.find { it.code.lowercase() == codeClass.lowercase() }

        if (configClass == null) {
            logger.error(
                CommonFunctions().createObjectIdForLogMsg(
                    codeClass,
                    "",
                    null,
                    -1
                ) + "\n" + "There is no description of class in configuration file. "
            )
            exitProcess(-1)
        }

        return configClass

    }

    // Поиск названия класса, на который есть ссылка в структуре scale в файле конфигурации
    // Если такого класса нет в конфигурации, то функция вернет ""
    public fun getClassNameByScaleKeyType(keyType: String): String {

        var scaleConfigClassName: String = ""
        var scaleConfigClass: ObjectCfg? = null

        // Любой класс, который в структуре scale в конфигурации ссылается на класс шкалы ссылкой InScale
        val configClassUsingScale =
            jsonConfigFileLocal.objects.find { configClass -> configClass.scale?.find { scaleRefClass -> scaleRefClass.keyType == keyType }?.codeRef != null }

        configClassUsingScale?.let { cfgClassUsingScale ->

            // Название класса, описывающего шкалу. В найденной структуре Scale в поле codeRef беру имя класса шкалы
            scaleConfigClassName = cfgClassUsingScale.scale?.find { it.keyType == keyType }?.codeRef ?: ""

            // Описание класса шкалы из файла конфигурации.
            if (scaleConfigClassName != "") {
                scaleConfigClass = getClassDescriptionByCode(scaleConfigClassName)
            }
        }

        return if (scaleConfigClass == null)
            ""
        else
            scaleConfigClassName.lowercase()
    }

    // Поиск класса описывающего шкалу в файле конфигурации. Поиск идет по ключу "keyType": "InScale" на класс шкалы.
    // Если такого класса нет в конфигурации, то функция вернет null
    public fun getScaleConfigClassDescription(): ObjectCfg? {

        var scaleConfigClass: ObjectCfg? = null

        val scaleConfigClassName = this.getClassNameByScaleKeyType("InScale")

        // Описание класса шкалы из файла конфигурации.
        if (scaleConfigClassName != "") {
            //scaleConfigClass = jsonConfigFileLocal.objects.find { it.code == scaleConfigClassName }
            scaleConfigClass = getClassDescriptionByCode(scaleConfigClassName)
        }

        return scaleConfigClass

    }

    // Проверка есть ли у переданного описания класса шкала. Поиск по ключу "keyType": "InScale".
    // Если шкала найдена, то вернёт true, иначе false
    public fun isClassHaveScale(
        configClass: ObjectCfg
    ): Boolean {

        return configClass.scale?.find { it.keyType == "InScale" } != null

    }

    // Проверка есть ли у класса переданного объекта связь с компонентом шкалы тарифа. Поиск по ключу "keyType": "InScaleComponent".
    // Если связь с компонентом найдена, то вернёт true, иначе false
    public fun isClassHaveScaleComponent(
        objectToLoad: DataDB
    ): Boolean {

        // описание класса из конфигурации
        val configClass = getClassDescriptionByCode(objectToLoad.code)

        // есть ли ссылка с ключом InScaleComponent
        return configClass.scale?.find { it.keyType == "InScaleComponent" } != null

    }

    // Проверка есть ли у переданного описания класса связь с компонентом шкалы тарифа. Поиск по ключу "keyType": "InScaleComponent".
    // Если связь с компонентом найдена, то вернёт true, иначе false
    public fun isClassHaveScaleComponent(
        configClass: ObjectCfg
    ): Boolean {

        // есть ли ссылка с ключом InScaleComponent
        return configClass.scale?.find { it.keyType == "InScaleComponent" } != null

    }

    // Поиск названия поля с идентификатором шкалы по классу из объекта и ключам keyType == "InScale" или "InParentScale"
    // Если поле не найдено, то вернет ""
    public fun getScaleIdFieldName(objectToLoad: DataDB): String {

        // описание класса из конфигурации
        //val configClass = jsonConfigFileLocal.objects.find { it.code == objectToLoad.code }
        val configClass = getClassDescriptionByCode(objectToLoad.code)

        val filedName = configClass.scale?.find { it.keyType == "InScale" || it.keyType == "InParentScale" }?.refField

        return filedName?.lowercase() ?: ""
    }

    // Поиск названия поля с идентификатором шкалы по описанию класса и ключам keyType == "InScale" или "InParentScale"
    // Если поле не найдено, то вернет ""
    public fun getScaleIdFieldName(configClass: ObjectCfg): String {

        val filedName = configClass.scale?.find { it.keyType == "InScale" || it.keyType == "InParentScale" }?.refField

        return filedName?.lowercase() ?: ""
    }

    // Поиск названия класса(scaleComponent) описывающего компонент шкалы тарифа в файле конфигурации.
    // Поиск идет в структуре scale по наличию ключа "keyType": "InScaleComponent", в котором указано названия искомого класса
    // Если такого класса нет в конфигурации, то функция вернет ""
    private fun getScaleComponentConfigClassName(): String {

        return getClassNameByScaleKeyType("InScaleComponent")

    }

    // Поиск названия класса(scaleComponentValue) описывающего связь компонента шкалы тарифа и измеряемой величины тарифа в файле конфигурации.
    // Поиск по ключу "keyType": "InScaleComponentValue" в структуре scale
    // Если такого класса нет в конфигурации, то функция вернет ""
    private fun getScaleComponentValueConfigClassName(): String {

        var scaleComponentValueConfigClassName: String = ""
        var scaleComponentValueConfigClass: ObjectCfg? = null

        // ищу класс(scaleComponent), описывающий компонент шкалы тарифа в файле конфигурации
        val scaleComponentConfigClassName = this.getScaleComponentConfigClassName()
        if (scaleComponentConfigClassName != "") {
            val scaleComponentConfigClass = this.getClassDescriptionByCode(scaleComponentConfigClassName)

            // в классе, описывающем компонент шкалы тарифа, ищу ссылку на класс, описывающий связь компонента шкалы тарифа и измеряемой величины тарифа
            scaleComponentValueConfigClassName =
                scaleComponentConfigClass.scale?.find { it.keyType == "InScaleComponentValue" }?.codeRef ?: ""

            // Описание класса шкалы из файла конфигурации.
            if (scaleComponentValueConfigClassName != "") {
                scaleComponentValueConfigClass = getClassDescriptionByCode(scaleComponentValueConfigClassName)
            }
        }
        return if (scaleComponentValueConfigClass == null)
            ""
        else
            scaleComponentValueConfigClassName.lowercase()
    }

    // Поиск названия класса(scalableAmount) описывающего измеряемую величину тарифа в файле конфигурации.
    // Поиск по ключу "keyType": "InScalableAmount" в структуре scale
    // Если такого класса нет в конфигурации, то функция вернет ""
    private fun getScalableAmountConfigClassName(): String {

        var scalableAmountConfigClassName: String = ""
        var scalableAmountConfigClass: ObjectCfg? = null

        // ищу класс(scaleComponent), описывающий компонент шкалы тарифа в файле конфигурации
        val scaleComponentValueConfigClassName = this.getScaleComponentValueConfigClassName()
        if (scaleComponentValueConfigClassName != "") {
            val scaleComponentValueConfigClass = this.getClassDescriptionByCode(scaleComponentValueConfigClassName)

            // в классе, описывающем компонент шкалы тарифа, ищу ссылку на класс, описывающий связь компонента шкалы тарифа и измеряемой величины тарифа
            scalableAmountConfigClassName =
                scaleComponentValueConfigClass.scale?.find { it.keyType == "InScalableAmount" }?.codeRef ?: ""

            // Описание класса шкалы из файла конфигурации.
            if (scalableAmountConfigClassName != "") {
                scalableAmountConfigClass = getClassDescriptionByCode(scalableAmountConfigClassName)
            }
        }

        return if (scalableAmountConfigClass == null)
            ""
        else
            scalableAmountConfigClassName.lowercase()
    }

    // Поиск названия поля по ключу "keyType" из ссылки в структуре scale
    // Если поле не найдено, то вернет ""
    private fun getScaleFieldNameByKeyType(keyType: String): String {

        var scaleComponentIdFieldName: String? = ""

        // Любой класс, который в структуре scale в конфигурации ссылается на класс шкалы ссылкой keyType
        val configClassUsingScale =
            jsonConfigFileLocal.objects.find { configClass -> configClass.scale?.find { scaleRefClass -> scaleRefClass.keyType == keyType }?.codeRef != null }

        configClassUsingScale?.let { cfgClassUsingScale ->

            // Название класса, описывающего шкалу. В найденной структуре Scale в поле codeRef беру имя класса шкалы
            scaleComponentIdFieldName = cfgClassUsingScale.scale?.find { it.keyType == keyType }?.refField

        }

        return scaleComponentIdFieldName?.lowercase() ?: ""
    }

}

