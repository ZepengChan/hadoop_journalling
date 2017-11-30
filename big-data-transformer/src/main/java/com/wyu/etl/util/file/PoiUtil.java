package com.wyu.etl.util.file;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * 表格对象创建类
 */
public class PoiUtil {

    public static Workbook getWorkbook(String filePath) {
        try {
            return WorkbookFactory.create(new FileInputStream(filePath));
        } catch (IOException | InvalidFormatException e) {
            e.printStackTrace();
        }
        return null;
    }

}
