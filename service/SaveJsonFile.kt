package com.solanteq.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.io.File

class WriterJson {

    // запись данных из БД в json
    /*fun createJsonFile(structData: List<*>, fileName : String) {
        val jsonString = jacksonObjectMapper().writeValueAsString(structData)
        val writer = File(fileName).bufferedWriter()
        writer.write(jsonString)
        writer.close()
    }*/

    fun createJsonFile(structData: TaskFileFieldsMain, fileName : String) {
        val jsonString = jacksonObjectMapper().writeValueAsString(structData)
        val writer = File(fileName).bufferedWriter()
        writer.write(jsonString)
        writer.close()
    }

    fun createJsonFile(structData: DataDBMain, fileName : String) {
        val jsonString = jacksonObjectMapper().writeValueAsString(structData)
        val writer = File(fileName).bufferedWriter()
        writer.write(jsonString)
        writer.close()
    }

    /*fun createJsonFile(attribute : ObjectNode) : String {
        return jacksonObjectMapper().writeValueAsString(attribute)
    }

    fun createJsonFile(attribute : ArrayNode) : String {
        return jacksonObjectMapper().writeValueAsString(attribute)
    }*/

    fun createJsonFile(attribute : JsonNode) : String {
        return jacksonObjectMapper().writeValueAsString(attribute)
    }


}
