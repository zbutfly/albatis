package net.butfly.albatis.bcp.imports.criterion;

import java.io.File;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code :
 * @since : Created in 14:50 2019/3/4
 */
public class FileUtils {
    public static void mkdir(String path) {
        File dir = new File(path);
        if (!dir.isDirectory()) {
            dir.mkdirs();
        }
        //dir = null;
    }

    public static void cleanDir(String path) {
        File dir = new File(path);
        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                } else if (file.isDirectory()) {
                    cleanDir(file.getAbsolutePath());
                    file.delete();
                }
            }
        }
    }

    public static void move(String srcPath, String dstPath) {
        File dir = new File(srcPath);
        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                file.renameTo(new File(dstPath + "/" + file.getName()));
            }
        }
    }

    /**
     * 2  * 删除目录（文件夹）以及目录下的文件
     * 3  * @param   sPath 被删除目录的文件路径
     * 4  * @return  目录删除成功返回true，否则返回false
     * 5
     */
    public static boolean deleteDirectory(String sPath) {
        boolean flag;
        //如果sPath不以文件分隔符结尾，自动添加文件分隔符
        if (!sPath.endsWith(File.separator)) {
            sPath = sPath + File.separator;
        }
        File dirFile = new File(sPath);
        //如果dir对应的文件不存在，或者不是一个目录，则退出
        if (!dirFile.exists() || !dirFile.isDirectory()) {
            return false;
        }
        flag = true;
        //删除文件夹下的所有文件(包括子目录)
        File[] files = dirFile.listFiles();
        for (int i = 0; i < files.length; i++) {
            //删除子文件
            if (files[i].isFile()) {
                flag = deleteFile(files[i].getAbsolutePath());
                if (!flag) break;
            } //删除子目录
            else {
                flag = deleteDirectory(files[i].getAbsolutePath());
                if (!flag) break;
            }
        }
        if (!flag) return false;
        //删除当前目录
        if (dirFile.delete()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 删除单个文件
     *
     * @param sPath 被删除文件的文件名
     * @return 单个文件删除成功返回true，否则返回false
     */
    public static boolean deleteFile(String sPath) {
        boolean flag = false;
        File file = new File(sPath);
        // 路径为文件且不为空则进行删除
        if (file.isFile() && file.exists()) {
            file.delete();
            flag = true;
        }
        return flag;
    }

}
