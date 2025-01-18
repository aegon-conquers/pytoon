package com.example.singlestorecsv.controller;

import com.example.singlestorecsv.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DataController {
    @Autowired
    private DataService dataService;

    @GetMapping("/export-csv")
    public ResponseEntity<byte[]> exportCsv() {
        String csvContent = dataService.fetchDataAsCsv();
        byte[] csvBytes = csvContent.getBytes();

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=data.csv")
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(csvBytes);
    }
}
