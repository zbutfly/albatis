package net.butfly.albatis.bcp.utils;

import net.butfly.albacore.utils.logger.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class UnZip {
    //public static Logger logger = Logger.getLogger(UnZip.class);

    public static void unZip(String filename, Path unzipath) {
        byte doc[] = null;
        FileOutputStream out = null;
        try{
            ZipInputStream zipis=new ZipInputStream(new FileInputStream(filename), StandardCharsets.UTF_8);
            ZipEntry fentry=null;
            while((fentry=zipis.getNextEntry())!=null) {
                if(fentry.isDirectory()){
                    File dir = unzipath.resolve(fentry.getName()).toFile();//new File(unzipath+fentry.getName());
                    if(!dir.exists()){
                        dir.mkdirs();
                    }
                }
                else {
                    //String fname=new String(unzipath+fentry.getName());
                    try{
                        out = new FileOutputStream(unzipath.resolve(fentry.getName()).toString());
                        doc=new byte[512];
                        int n;
                        //若没有读到，即读取到末尾，则返回-1
                        while((n=zipis.read(doc,0,512))!=-1) {
                            out.write(doc,0,n);
                        }

                    }catch (Exception ex) {
                        //logger.error("unzip error:" + ex);
                    }finally {
                        out.close();
                        out=null;
                        doc=null;
                    }
                }
            }
            zipis.close();
        }catch (IOException ioex){
            //logger.error("unzip IOException:" + ioex);
        }
    }

}
