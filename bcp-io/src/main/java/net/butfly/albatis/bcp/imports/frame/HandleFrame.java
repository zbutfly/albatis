package net.butfly.albatis.bcp.imports.frame;


import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.bcp.TaskDesc;
import net.butfly.albatis.bcp.imports.criterion.Criterion;
import net.butfly.albatis.bcp.imports.frame.reader.ReadFromDirectory;
import net.butfly.albatis.bcp.imports.frame.reader.ReadFromFile;
import net.butfly.albatis.bcp.imports.frame.reader.local.ReadFromLocalDirectory;
import net.butfly.albatis.bcp.imports.frame.reader.local.ReadFromLocalFile;
import net.butfly.albatis.bcp.imports.frame.struct.KernelInfo;
import net.butfly.albatis.bcp.utils.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code : @since : Created in 15:34 2019/3/1
 */
public class HandleFrame implements Runnable {
    private final static Logger LOGGER = LoggerFactory.getLogger(HandleFrame.class);

    // 存储配置信息
    private KernelInfo kernelInfo = null;
    private boolean loopFlag = false;

    private ReadFromDirectory rfd = null;
    private ReadFromFile rff = null;

    private int inputNum = 0;
    private int errorNum = 0;
    private int outputNum = 0;

    private final int PERSENT = 100;
    private int mixCount = 0;
    private int[] mixRateArray = null;
    private int outputCount = 0;
    private int[] outputRateArray = null;
    private String[] transFunctionArray = null;

    private long lastLogTime = 0;
    private URISpec uri;
    private String table;

    public HandleFrame(KernelInfo kernelInfo, boolean loopFlag, URISpec uri, String table) {
        this.kernelInfo = kernelInfo;
        this.loopFlag = loopFlag;
        this.uri = uri;
        this.table = table;
        init();
    }

    private void init() {
        switch (this.kernelInfo.getInputProtocol()) {
            case "file":
                rfd = new ReadFromLocalDirectory();
                rff = new ReadFromLocalFile();
                break;
            default:
                break;
        }

        mixRateArray = new int[this.kernelInfo.getFields().size()];
        outputRateArray = new int[this.kernelInfo.getFields().size()];
        transFunctionArray = new String[this.kernelInfo.getFields().size()];

        for (int i = 0; i < mixRateArray.length; i++) {
            mixRateArray[i] = this.kernelInfo.getFields().get(i).getMixRate();
            outputRateArray[i] = this.kernelInfo.getFields().get(i).getOutputRate();
            transFunctionArray[i] = this.kernelInfo.getFields().get(i).getTransFunction();
        }

    }

    @Override
    public void run() {
        LOGGER.debug("Starting to handle DataEName:" + this.kernelInfo.getInputDataEName());
        try (Criterion criterion = new Criterion(this.kernelInfo, uri, table)) {
            do { // 框架代码
                List<String> files = rfd.getFiles(this.kernelInfo.getInputPath());
                if (files.size() != 0) {
                    // 设置行列分隔符,可以修改成根据配置
                    rff.setFieldSplit(this.kernelInfo.getIntputFiledSplit());
                    rff.setLineSplit(this.kernelInfo.getIntputLineSplit());

                    for (String file : files) {
                        rff.load(file);
                        while (rff.hasNext()) {
                            String[] fields = rff.next();
                            inputNum++;
                            if (fields == null || fields.length != this.kernelInfo.getFields().size()) {
                                errorNum++;
                                continue;
                            }

                            // 处理字段
                            handleFields(fields);

                            // 结果输出到部标准文件中
                            criterion.write(fields);
                            outputNum++;
                        }
                    }

                } else try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (System.currentTimeMillis() / 1000 - lastLogTime > 30) {
                    LOGGER.trace("inputNum:" + inputNum + "\toutputNum:" + outputNum + "\terrorNum:" + errorNum);
                    lastLogTime = System.currentTimeMillis() / 1000;
                }
            } while (this.loopFlag);
            LOGGER.trace("inputNum:" + inputNum + "\toutputNum:" + outputNum + "\terrorNum:" + errorNum
                    + "\n\tiHandleFrame (handling DataEName:" + this.kernelInfo.getInputDataEName() + ") is over.");
        }

    }

    private void handleFields(String[] fields) {
        // 对于每个字段都做对应的处理
        for (int i = 0; i < fields.length; i++) {
            // 判断当前字段是否需要输出
            outputCount = (outputCount + 1) % PERSENT;
            if (outputCount <= outputRateArray[i]) {

                // 如果当前字段需要混淆
                mixCount = (mixCount + 1) % PERSENT;
                if (mixCount < mixRateArray[i]) {
                    // 使用混淆函数
                    fields[i] = Format.baseMix(fields[i]);
                } else if (!transFunctionArray[i].equals("")) {
                    // 不混淆，则对字段进行处理
                    // 使用配置的函数对字段进行处理
                    try {
                        Method method = Format.class.getMethod(transFunctionArray[i], String.class);
                        fields[i] = (String) method.invoke(null, fields[i]);
                    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                        fields[i] = "";
                        e.printStackTrace();
                    }
                }
            } else {
                // 不输出 ，直接赋值成空值
                fields[i] = "";
            }
        }
    }
}
