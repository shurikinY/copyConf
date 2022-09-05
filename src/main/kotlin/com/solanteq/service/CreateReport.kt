package com.solanteq.service

import org.slf4j.LoggerFactory

/*
Создание сводного отчета по результатам режима check/load
*/


// объект класса хранит информацию о количестве добавленных/измененных/пропущенных объектах
data class ReportData(
    var classCode: String,
    var numberOfObjectInserted: Int,
    var numberOfObjectUpdated: Int,
    var numberOfObjectSkipped: Int
)


class CreateReport {

    private val logger = LoggerFactory.getLogger(javaClass)

    private var resultListOfProcessedObject = mutableListOf<ReportData>()

    // Формирование сводного отчета по результатам проверки/загрузки файла
    public fun createSummaryReport(
        classCode: String,
        actionWithObject: ActionWithObject
    ) {

        var resultLoad = ReportData(classCode, 0, 0, 0)

        val indexOfClassCode = resultListOfProcessedObject.indexOfFirst { it.classCode == classCode }
        if (indexOfClassCode > -1) {
            resultLoad = resultListOfProcessedObject[indexOfClassCode]
        }

        if (actionWithObject.actionInsert)
            resultLoad.numberOfObjectInserted++
        else if (actionWithObject.actionUpdateMainRecord || actionWithObject.actionUpdateRefTables || actionWithObject.actionUpdateLinkRecord)
            resultLoad.numberOfObjectUpdated++
        else
            resultLoad.numberOfObjectSkipped++

        if (indexOfClassCode == -1) {
            resultListOfProcessedObject.add(resultLoad)
        }

    }

    // Печать сводного отчета
    public fun writeReportOfProgramExecution(typeReport : String) {

        logger.info("Summary report:")

        resultListOfProcessedObject.sortWith(compareBy {it.classCode})

        for (processedObject in resultListOfProcessedObject) {
            if (typeReport == "check") {
                logger.info(
                    "Object class code <${processedObject.classCode}>: " +
                            "number of objects to be inserted <${processedObject.numberOfObjectInserted}>, " +
                            "number of objects to be updated <${processedObject.numberOfObjectUpdated}>, " +
                            "number of objects to be skipped <${processedObject.numberOfObjectSkipped}>"
                )
            } else {
                logger.info(
                    "Object class code <${processedObject.classCode}>: " +
                            "number of inserted objects <${processedObject.numberOfObjectInserted}>, " +
                            "number of updated objects <${processedObject.numberOfObjectUpdated}>, " +
                            "number of skipped objects <${processedObject.numberOfObjectSkipped}>"
                )
            }
        }

    }

}