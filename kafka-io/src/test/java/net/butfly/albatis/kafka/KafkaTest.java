package net.butfly.albatis.kafka;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.serder.BsonSerder;

public class KafkaTest {
	public static void main(String[] args) throws ConfigException, IOException {
		if (args.length != 2) throw new RuntimeException("KafkaTest <uri> <batchSize> , topics in url");
		// int batchSize = Integer.parseInt(args[1]);

		Map<String, Integer> counts = new HashMap<>();
		NumberFormat nf = new DecimalFormat("0.00");
		DateFormat df = new SimpleDateFormat("MM-dd hh:mm:ss ");
		double total = 0;
		long begin = new Date().getTime();
		long now = begin;
		try (KafkaInput in = new KafkaInput("KafkaIn", args[0], b -> BsonSerder.map(b));) {
			do {
				AtomicInteger count = new AtomicInteger(0);
				AtomicLong size = new AtomicLong();
				in.dequeue(s -> size.addAndGet(s.map(m -> {
					count.incrementAndGet();
					counts.compute(m.table(), (k, v) -> v + 1);
					return m.toBytes().length;
				}).reduce((i1, i2) -> i1 + i2)), batchSize);
				long curr = new Date().getTime();
				total += (size.get() / 1024.0 / 1024);
				System.out.println(df.format(new Date()) + "<Count: " + count.get() + ">: <" + nf.format((curr - now) / 1000.0) + " secs>, "//
						+ "size: <" + nf.format(size.get() / 1024.0 / 1024) + " MByte>, "//
						+ "total: <" + nf.format(total / 1024) + " Gbytes>, "//
						+ "avg: <" + nf.format(total / ((curr - begin) / 1000.0)) + " MBytes/sec>, " //
						// + "pool: <" + in.poolStatus() + ">."//
						+ "\n\tcount detail: <" + counts + ">, " //
				);
				now = new Date().getTime();
			} while (true);
		}
	}

	public static final String[] RELATION_TOPICS = new String[] { "RELATION_PERSON_CASE", "RELATION_PERSON_CYBERCAFE",
			"RELATION_PERSON_FLIGHT_2", "RELATION_PERSON_HOTEL_ROOM", "RELATION_PERSON_HOUSEHOLD", "RELATION_PERSON_PECCANCY",
			"RELATION_PERSON_SAMECONTACT", "RELATION_PERSON_TJS", "RELATION_PERSON_TZZ" };
	public static final String[] HZGA_TOPICS = new String[] { "HZGA_GAZHK_BASEPOLICE_BUILDING", "HZGA_GAZHK_BASEPOLICE_DWCYRY",
			"HZGA_GAZHK_BASEPOLICE_DWFC", "HZGA_GAZHK_BASEPOLICE_RECORD_BIKE", "HZGA_GAZHK_BASEPOLICE_RECORD_CAR",
			"HZGA_GAZHK_BASEPOLICE_RECORD_RESIDENT", "HZGA_GAZHK_BASEPOLICE_ROOM", "HZGA_GAZHK_BASEPOLICE_RYFC",
			"HZGA_GAZHK_BASEPOLICE_ZLFWXX", "HZGA_GAZHK_BDQ_CXB_II", "HZGA_GAZHK_BDQ_CXB_QG_II", "HZGA_GAZHK_BDQ_CXB_ZJ",
			"HZGA_GAZHK_BDQ_DJB_II", "HZGA_GAZHK_BDQ_DJB_QG", "HZGA_GAZHK_BDQ_DJB_QG_II", "HZGA_GAZHK_BDQ_DJB_ZJ", "HZGA_GAZHK_CGBC_BWYWMX",
			"HZGA_GAZHK_CRJGL_SWQBXX", "HZGA_GAZHK_CRJXT_SQXX", "HZGA_GAZHK_CZRK", "HZGA_GAZHK_DFK_AJXX", "HZGA_GAZHK_DFK_RYXX",
			"HZGA_GAZHK_DFK_WFFZRY", "HZGA_GAZHK_DFK_WPXX", "HZGA_GAZHK_DFK_WP_CLXX", "HZGA_GAZHK_DSDB_CJD", "HZGA_GAZHK_DSDB_FKD",
			"HZGA_GAZHK_DSDB_JJDB", "HZGA_GAZHK_DT_YKT_KLXX", "HZGA_GAZHK_DT_YPT_KLXX", "HZGA_GAZHK_DWXX_GAJKDW", "HZGA_GAZHK_DWXX_KKDW",
			"HZGA_GAZHK_DZZWB_CGXZCF", "HZGA_GAZHK_DZZWB_DMSB", "HZGA_GAZHK_DZZWB_DSQYXX", "HZGA_GAZHK_DZZWB_FWQSXX",
			"HZGA_GAZHK_DZZWB_FY_DWSXXX", "HZGA_GAZHK_DZZWB_FY_GRSXXX", "HZGA_GAZHK_DZZWB_GJJDKXX", "HZGA_GAZHK_DZZWB_GJJXX",
			"HZGA_GAZHK_DZZWB_GSQYXX", "HZGA_GAZHK_DZZWB_GS_ESCFPXX", "HZGA_GAZHK_DZZWB_LW_DYCYRY", "HZGA_GAZHK_DZZWB_MFSTDWXX",
			"HZGA_GAZHK_DZZWB_MFSTRYXX", "HZGA_GAZHK_DZZWB_MZ_BJXX", "HZGA_GAZHK_DZZWB_MZ_BLXX", "HZGA_GAZHK_DZZWB_MZ_DBXX",
			"HZGA_GAZHK_DZZWB_MZ_JHXX", "HZGA_GAZHK_DZZWB_MZ_JJHDJ", "HZGA_GAZHK_DZZWB_MZ_LHXX", "HZGA_GAZHK_DZZWB_MZ_ZCXX",
			"HZGA_GAZHK_DZZWB_RS_NDLDBZSMSC", "HZGA_GAZHK_DZZWB_RS_XEDBDKHMD", "HZGA_GAZHK_DZZWB_RS_XZCF_DW", "HZGA_GAZHK_DZZWB_RS_XZCF_GR",
			"HZGA_GAZHK_DZZWB_RS_ZGCBXX", "HZGA_GAZHK_DZZWB_SCJG_CFXX", "HZGA_GAZHK_DZZWB_SCJG_QYHZNR", "HZGA_GAZHK_DZZWB_SF_FLFWGZZJBXX",
			"HZGA_GAZHK_DZZWB_SF_SFJDRJBXX", "HZGA_GAZHK_ELES_AJ_TBB", "HZGA_GAZHK_ELES_AJ_TBB_RY", "HZGA_GAZHK_ELES_BLXX",
			"HZGA_GAZHK_ELES_CASES", "HZGA_GAZHK_ELES_CASE_RETAINERS", "HZGA_GAZHK_ELES_CASE_SUSPECTS", "HZGA_GAZHK_ELES_RYAJ",
			"HZGA_GAZHK_ELES_RY_BZCJRYBBB", "HZGA_GAZHK_ELES_RY_TBB", "HZGA_GAZHK_ELES_SQBZJB", "HZGA_GAZHK_GAUSER_RCJZZRY",
			"HZGA_GAZHK_GAUSER_XX_JTCY", "HZGA_GAZHK_GAUSER_XX_RYXX", "HZGA_GAZHK_HZKK", "HZGA_GAZHK_HZZD_FWZHRY",
			"HZGA_GAZHK_HZZD_LDRKFWXX", "HZGA_GAZHK_HZ_ZJK1_AJXX", "HZGA_GAZHK_HZ_ZJK1_BAQRYXX", "HZGA_GAZHK_HZ_ZJK1_QSYJS",
			"HZGA_GAZHK_HZ_ZJK1_RAGL", "HZGA_GAZHK_HZ_ZJK1_WPXX", "HZGA_GAZHK_HZ_ZJK1_XYRXX", "HZGA_GAZHK_HZ_ZJK1_YDCJRY",
			"HZGA_GAZHK_JGXT_JDSRYXX", "HZGA_GAZHK_JGXT_JLSRYXX", "HZGA_GAZHK_JGXT_KSSJSHJB", "HZGA_GAZHK_JGXT_KSSRYXX",
			"HZGA_GAZHK_JGXT_SJSRYXX", "HZGA_GAZHK_JJZD_CLWHPXX", "HZGA_GAZHK_JJZD_FDBACLXX", "HZGA_GAZHK_JJZD_FXCWBJLB",
			"HZGA_GAZHK_JJZD_JSZKSXYXX", "HZGA_GAZHK_JJZD_JYSGXX", "HZGA_GAZHK_JJZD_JYSGXXB", "HZGA_GAZHK_JJZD_QZCSXX",
			"HZGA_GAZHK_JJZD_SGDSRXX", "HZGA_GAZHK_JJZD_SGXXB", "HZGA_GAZHK_JJZD_WFJLXX", "HZGA_GAZHK_JJZD_YBSGXX", "HZGA_GAZHK_JSZXX",
			"HZGA_GAZHK_JTXT_FJDC", "HZGA_GAZHK_JTXT_HZJDCXX", "HZGA_GAZHK_JWBZD_WZXSXX", "HZGA_GAZHK_JWGZPT1_JYXX",
			"HZGA_GAZHK_JWGZPT1_QSJYXX", "HZGA_GAZHK_JZWL_KDSJ_HZ_T", "HZGA_GAZHK_KDSJ_2011", "HZGA_GAZHK_KDSJ_2012",
			"HZGA_GAZHK_KDSJ_2013", "HZGA_GAZHK_KDSJ_2014", "HZGA_GAZHK_KDSJ_2015", "HZGA_GAZHK_KDSJ_2016", "HZGA_GAZHK_LGY_JWRYXX",
			"HZGA_GAZHK_LGY_LGXX", "HZGA_GAZHK_LGY_NB", "HZGA_GAZHK_LGY_WB", "HZGA_GAZHK_LMSZQYXX", "HZGA_GAZHK_MHSJ_MHJCGSJ_1",
			"HZGA_GAZHK_MHSJ_MHJCG_NEW", "HZGA_GAZHK_NEWZZRK_ZLFWXX_TAB", "HZGA_GAZHK_OLDWF_WFJB", "HZGA_GAZHK_PERSON",
			"HZGA_GAZHK_QBPT_CRJRYZJXX", "HZGA_GAZHK_QBPT_FWCZXX", "HZGA_GAZHK_QBPT_FWPZXX", "HZGA_GAZHK_QBPT_IHANGZHOU",
			"HZGA_GAZHK_QBPT_JD_SDAJ", "HZGA_GAZHK_QBPT_JD_SDXYR", "HZGA_GAZHK_QBPT_JD_XDRYDTGK", "HZGA_GAZHK_QBPT_LGSWXNGJ",
			"HZGA_GAZHK_QBPT_QQBZXX", "HZGA_GAZHK_QBPT_SPXXPT_DWXX", "HZGA_GAZHK_QBPT_TBSHDZ", "HZGA_GAZHK_QBPT_XJQKRY",
			"HZGA_GAZHK_QBPT_ZDRYJBXX", "HZGA_GAZHK_QBPT_ZDRYYJXX", "HZGA_GAZHK_QBPT_ZDRY_BDGK", "HZGA_GAZHK_SHXXCJ_CQLNTCBW",
			"HZGA_GAZHK_SHXXCJ_SZGCF", "HZGA_GAZHK_SHXXCJ_ZHKYCL", "HZGA_GAZHK_SJZYZH_GHJPOI", "HZGA_GAZHK_SJZYZH_GLKCRH",
			"HZGA_GAZHK_SJZYZH_HZDDSJ", "HZGA_GAZHK_SJZYZH_HZHBQYXX", "HZGA_GAZHK_SJZYZH_HZYBSJ", "HZGA_GAZHK_SJZYZH_LJFWZLXX",
			"HZGA_GAZHK_SJZYZH_LMSZQY", "HZGA_GAZHK_SJZYZH_MHSJ_ST", "HZGA_GAZHK_SJZYZH_MSZSJL", "HZGA_GAZHK_SJZYZH_PASSENGER_INFO_TAB",
			"HZGA_GAZHK_SJZYZH_QBTB_RCHL_CLHC", "HZGA_GAZHK_SJZYZH_QBTB_RCHL_RYHC", "HZGA_GAZHK_SJZYZH_TL_DPXX_ST",
			"HZGA_GAZHK_SJZYZH_T_HUOCHE", "HZGA_GAZHK_SJZYZH_WYCLXX", "HZGA_GAZHK_SJZYZH_WYSJXX", "HZGA_GAZHK_SJZYZH_WZCKCRJ",
			"HZGA_GAZHK_SJZYZH_WZXSCRJ", "HZGA_GAZHK_SJZYZH_WZZKCRJ", "HZGA_GAZHK_SSSJ_BZDZ", "HZGA_GAZHK_SSSJ_SYRK",
			"HZGA_GAZHK_T_CAR_WORKORDER", "HZGA_GAZHK_WBXT_SWRY_XXB", "HZGA_GAZHK_WBXT_XNSF", "HZGA_GAZHK_XF_CF", "HZGA_GAZHK_XF_HD",
			"HZGA_GAZHK_XF_QZ", "HZGA_GAZHK_XF_XK", "HZGA_GAZHK_XF_YH", "HZGA_GAZHK_XNSF_SWXNSF", "HZGA_GAZHK_XZTX_QCT_DXJL",
			"HZGA_GAZHK_XZTX_QCT_HD", "HZGA_GAZHK_XZTX_QCT_JZXX", "HZGA_GAZHK_XZTX_QCT_TXL", "HZGA_GAZHK_XZTX_XNSF_MOMOACCOUNT",
			"HZGA_GAZHK_XZTX_XNSF_QQACCOUNT", "HZGA_GAZHK_XZTX_XNSF_WANGXINACCOUNT", "HZGA_GAZHK_XZTX_XNSF_WEIBOACCOUNT",
			"HZGA_GAZHK_XZTX_XNSF_WXACCOUNT", "HZGA_GAZHK_YCSBT_SBXX_JB", "HZGA_GAZHK_YDJW_AJHC", "HZGA_GAZHK_YDJW_CLHC",
			"HZGA_GAZHK_YDJW_FJDCCX", "HZGA_GAZHK_YDJW_FJDCHC", "HZGA_GAZHK_YDJW_JDCCX", "HZGA_GAZHK_YDJW_PHOTO", "HZGA_GAZHK_YDJW_RYCX",
			"HZGA_GAZHK_YDJW_RYHC", "HZGA_GAZHK_YDJW_XSJL", "HZGA_GAZHK_ZAZD_BAYZXX", "HZGA_GAZHK_ZAZD_CYBBDWXX", "HZGA_GAZHK_ZAZD_DDHCYRY",
			"HZGA_GAZHK_ZAZD_DDHXX", "HZGA_GAZHK_ZAZD_DHXXB", "HZGA_GAZHK_ZAZD_DWXXB", "HZGA_GAZHK_ZAZD_THQYXX", "HZGA_GAZHK_ZAZD_YLCSCYRY",
			"HZGA_GAZHK_ZAZD_YLCSDWXX", "HZGA_GAZHK_ZAZD_YZSPD", "HZGA_GAZHK_ZFZYK_BYSXX", "HZGA_GAZHK_ZFZYK_GSGTHJBXX",
			"HZGA_GAZHK_ZFZYK_HSKD_JFZH", "HZGA_GAZHK_ZFZYK_HSKD_KHB", "HZGA_GAZHK_ZFZYK_HSKD_YHB", "HZGA_GAZHK_ZFZYK_SBDWXX",
			"HZGA_GAZHK_ZFZYK_SBRYXX", "HZGA_GAZHK_ZFZYK_SBXX", "HZGA_GAZHK_ZFZYK_SWJ_DSQYXX", "HZGA_GAZHK_ZFZYK_SWJ_GSQYXX",
			"HZGA_GAZHK_ZFZYK_SZDS_JFZH", "HZGA_GAZHK_ZFZYK_SZDS_KHB", "HZGA_GAZHK_ZFZYK_SZDS_YHB", "HZGA_GAZHK_ZFZYK_TL_TICKET",
			"HZGA_GAZHK_ZFZYK_ZX_CARD", "HZGA_GAZHK_ZJK1_WPXX", "HZGA_GAZHK_ZTRY_CXRY_JBXX_II", "HZGA_GAZHK_ZTRY_QG_CXRY_JBXX_II",
			"HZGA_GAZHK_ZTRY_QG_ZTRY_JBXX_II", "HZGA_GAZHK_ZTRY_ZTRY_JBXX_II", "HZGA_GAZHK_ZZRK", "HZGA_GAZHK_ZZRK_HIS" };
}
