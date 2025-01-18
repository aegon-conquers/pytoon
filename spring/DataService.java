package com.example.singlestorecsv.service;

import com.example.singlestorecsv.entity.DataRecord;
import com.example.singlestorecsv.repository.DataRecordRepository;
import com.opencsv.CSVWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.StringWriter;
import java.util.List;

@Service
public class DataService {
    @Autowired
    private DataRecordRepository repository;

    public String fetchDataAsCsv() {
        List<DataRecord> dataRecords = repository.findAll();

        try (StringWriter stringWriter = new StringWriter();
             CSVWriter csvWriter = new CSVWriter(stringWriter)) {

            // Write header
            csvWriter.writeNext(new String[]{"ID", "Column1", "Column2", "Column3"});

            // Write data
            for (DataRecord record : dataRecords) {
                csvWriter.writeNext(new String[]{
                        String.valueOf(record.getId()),
                        record.getColumn1(),
                        record.getColumn2(),
                        record.getColumn3()
                });
            }

            return stringWriter.toString();
        } catch (Exception e) {
            throw new RuntimeException("Error generating CSV", e);
        }
    }
}
