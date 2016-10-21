package org.talend.components.performance.filedelimited;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import routines.TalendString;
import routines.system.TDieException;

public class FileOutputDelimitedProcess {
    
    public static final int LINES_TO_GENERATE = 1000000; 

    public void tRowGenerator_2Process(final java.util.Map<String, Object> globalMap) throws TalendException {
        globalMap.put("tRowGenerator_2_SUBPROCESS_STATE", 0);
        
        String iterateId = "";

        String currentComponent = "";
        java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

        try {

            String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();

                row2Struct row2 = new row2Struct();

                /**
                 * [tFileOutputDelimited_2 begin ] start
                 */

                int tos_count_tFileOutputDelimited_2 = 0;

                String fileName_tFileOutputDelimited_2 = "";
                fileName_tFileOutputDelimited_2 = (new java.io.File("D:/TOSWorkspace/Performance/out.csv")).getAbsolutePath()
                        .replace("\\", "/");
                String fullName_tFileOutputDelimited_2 = null;
                String extension_tFileOutputDelimited_2 = null;
                String directory_tFileOutputDelimited_2 = null;
                if ((fileName_tFileOutputDelimited_2.indexOf("/") != -1)) {
                    if (fileName_tFileOutputDelimited_2.lastIndexOf(".") < fileName_tFileOutputDelimited_2.lastIndexOf("/")) {
                        fullName_tFileOutputDelimited_2 = fileName_tFileOutputDelimited_2;
                        extension_tFileOutputDelimited_2 = "";
                    } else {
                        fullName_tFileOutputDelimited_2 = fileName_tFileOutputDelimited_2.substring(0,
                                fileName_tFileOutputDelimited_2.lastIndexOf("."));
                        extension_tFileOutputDelimited_2 = fileName_tFileOutputDelimited_2
                                .substring(fileName_tFileOutputDelimited_2.lastIndexOf("."));
                    }
                    directory_tFileOutputDelimited_2 = fileName_tFileOutputDelimited_2.substring(0,
                            fileName_tFileOutputDelimited_2.lastIndexOf("/"));
                } else {
                    if (fileName_tFileOutputDelimited_2.lastIndexOf(".") != -1) {
                        fullName_tFileOutputDelimited_2 = fileName_tFileOutputDelimited_2.substring(0,
                                fileName_tFileOutputDelimited_2.lastIndexOf("."));
                        extension_tFileOutputDelimited_2 = fileName_tFileOutputDelimited_2
                                .substring(fileName_tFileOutputDelimited_2.lastIndexOf("."));
                    } else {
                        fullName_tFileOutputDelimited_2 = fileName_tFileOutputDelimited_2;
                        extension_tFileOutputDelimited_2 = "";
                    }
                    directory_tFileOutputDelimited_2 = "";
                }
                boolean isFileGenerated_tFileOutputDelimited_2 = true;
                java.io.File filetFileOutputDelimited_2 = new java.io.File(fileName_tFileOutputDelimited_2);
                globalMap.put("tFileOutputDelimited_2_FILE_NAME", fileName_tFileOutputDelimited_2);
                int nb_line_tFileOutputDelimited_2 = 0;
                int splitEvery_tFileOutputDelimited_2 = 1000;
                int splitedFileNo_tFileOutputDelimited_2 = 0;
                int currentRow_tFileOutputDelimited_2 = 0;
                
                final String OUT_DELIM_tFileOutputDelimited_2 = /**
                                                                 * Start field
                                                                 * tFileOutputDelimited_2:FIELDSEPARATOR
                                                                 */
                        ";"/** End field tFileOutputDelimited_2:FIELDSEPARATOR */
                ;

                final String OUT_DELIM_ROWSEP_tFileOutputDelimited_2 = /**
                                                                        * Start
                                                                        * field tFileOutputDelimited_2:ROWSEPARATOR
                                                                        */
                        "\n"/** End field tFileOutputDelimited_2:ROWSEPARATOR */
                ;

                // create directory only if not exists
                if (directory_tFileOutputDelimited_2 != null && directory_tFileOutputDelimited_2.trim().length() != 0) {
                    java.io.File dir_tFileOutputDelimited_2 = new java.io.File(directory_tFileOutputDelimited_2);
                    if (!dir_tFileOutputDelimited_2.exists()) {
                        dir_tFileOutputDelimited_2.mkdirs();
                    }
                }

                // routines.system.Row
                java.io.Writer outtFileOutputDelimited_2 = null;

                java.io.File fileToDelete_tFileOutputDelimited_2 = new java.io.File(fileName_tFileOutputDelimited_2);
                if (fileToDelete_tFileOutputDelimited_2.exists()) {
                    fileToDelete_tFileOutputDelimited_2.delete();
                }
                outtFileOutputDelimited_2 = new java.io.BufferedWriter(new java.io.OutputStreamWriter(
                        new java.io.FileOutputStream(fileName_tFileOutputDelimited_2, false), "US-ASCII"));

                resourceMap.put("out_tFileOutputDelimited_2", outtFileOutputDelimited_2);
                resourceMap.put("nb_line_tFileOutputDelimited_2", nb_line_tFileOutputDelimited_2);

                /**
                 * [tFileOutputDelimited_2 begin ] stop
                 */

                /**
                 * [tRowGenerator_2 begin ] start
                 */

                currentComponent = "tRowGenerator_2";

                int tos_count_tRowGenerator_2 = 0;

                int nb_line_tRowGenerator_2 = 0;
                int nb_max_row_tRowGenerator_2 = LINES_TO_GENERATE;

                class tRowGenerator_2Randomizer {

                    public String getRandomColumn1() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn2() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn3() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn4() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn5() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn6() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn7() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn8() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn9() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn10() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn11() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn12() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn13() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn14() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn15() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn16() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn17() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn18() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn19() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn20() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn21() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn22() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn23() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn24() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn25() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn26() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn27() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn28() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn29() {

                        return TalendString.getAsciiRandomString(6);

                    }

                    public String getRandomColumn30() {

                        return TalendString.getAsciiRandomString(6);

                    }
                }
                tRowGenerator_2Randomizer randtRowGenerator_2 = new tRowGenerator_2Randomizer();

                for (int itRowGenerator_2 = 0; itRowGenerator_2 < nb_max_row_tRowGenerator_2; itRowGenerator_2++) {
                    row2.Column1 = randtRowGenerator_2.getRandomColumn1();
                    row2.Column2 = randtRowGenerator_2.getRandomColumn2();
                    row2.Column3 = randtRowGenerator_2.getRandomColumn3();
                    row2.Column4 = randtRowGenerator_2.getRandomColumn4();
                    row2.Column5 = randtRowGenerator_2.getRandomColumn5();
                    row2.Column6 = randtRowGenerator_2.getRandomColumn6();
                    row2.Column7 = randtRowGenerator_2.getRandomColumn7();
                    row2.Column8 = randtRowGenerator_2.getRandomColumn8();
                    row2.Column9 = randtRowGenerator_2.getRandomColumn9();
                    row2.Column10 = randtRowGenerator_2.getRandomColumn10();
                    row2.Column11 = randtRowGenerator_2.getRandomColumn11();
                    row2.Column12 = randtRowGenerator_2.getRandomColumn12();
                    row2.Column13 = randtRowGenerator_2.getRandomColumn13();
                    row2.Column14 = randtRowGenerator_2.getRandomColumn14();
                    row2.Column15 = randtRowGenerator_2.getRandomColumn15();
                    row2.Column16 = randtRowGenerator_2.getRandomColumn16();
                    row2.Column17 = randtRowGenerator_2.getRandomColumn17();
                    row2.Column18 = randtRowGenerator_2.getRandomColumn18();
                    row2.Column19 = randtRowGenerator_2.getRandomColumn19();
                    row2.Column20 = randtRowGenerator_2.getRandomColumn20();
                    row2.Column21 = randtRowGenerator_2.getRandomColumn21();
                    row2.Column22 = randtRowGenerator_2.getRandomColumn22();
                    row2.Column23 = randtRowGenerator_2.getRandomColumn23();
                    row2.Column24 = randtRowGenerator_2.getRandomColumn24();
                    row2.Column25 = randtRowGenerator_2.getRandomColumn25();
                    row2.Column26 = randtRowGenerator_2.getRandomColumn26();
                    row2.Column27 = randtRowGenerator_2.getRandomColumn27();
                    row2.Column28 = randtRowGenerator_2.getRandomColumn28();
                    row2.Column29 = randtRowGenerator_2.getRandomColumn29();
                    row2.Column30 = randtRowGenerator_2.getRandomColumn30();
                    nb_line_tRowGenerator_2++;

                    /**
                     * [tRowGenerator_2 begin ] stop
                     */

                    /**
                     * [tRowGenerator_2 main ] start
                     */

                    currentComponent = "tRowGenerator_2";

                    tos_count_tRowGenerator_2++;

                    /**
                     * [tRowGenerator_2 main ] stop
                     */
                    
                    /**
                     * [tFileOutputDelimited_2 main ] start
                     */

                    currentComponent = "tFileOutputDelimited_2";

                    StringBuilder sb_tFileOutputDelimited_2 = new StringBuilder();
                    if (row2.Column1 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column1);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column2 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column2);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column3 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column3);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column4 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column4);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column5 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column5);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column6 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column6);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column7 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column7);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column8 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column8);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column9 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column9);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column10 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column10);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column11 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column11);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column12 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column12);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column13 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column13);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column14 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column14);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column15 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column15);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column16 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column16);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column17 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column17);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column18 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column18);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column19 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column19);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column20 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column20);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column21 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column21);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column22 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column22);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column23 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column23);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column24 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column24);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column25 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column25);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column26 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column26);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column27 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column27);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column28 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column28);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column29 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column29);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_tFileOutputDelimited_2);
                    if (row2.Column30 != null) {
                        sb_tFileOutputDelimited_2.append(row2.Column30);
                    }
                    sb_tFileOutputDelimited_2.append(OUT_DELIM_ROWSEP_tFileOutputDelimited_2);

                    nb_line_tFileOutputDelimited_2++;
                    resourceMap.put("nb_line_tFileOutputDelimited_2", nb_line_tFileOutputDelimited_2);

                    outtFileOutputDelimited_2.write(sb_tFileOutputDelimited_2.toString());

                    tos_count_tFileOutputDelimited_2++;

                    /**
                     * [tFileOutputDelimited_2 main ] stop
                     */

                    /**
                     * [tRowGenerator_2 end ] start
                     */

                    currentComponent = "tRowGenerator_2";

                }
                globalMap.put("tRowGenerator_2_NB_LINE", nb_line_tRowGenerator_2);

                /**
                 * [tRowGenerator_2 end ] stop
                 */

                /**
                 * [tFileOutputDelimited_2 end ] start
                 */

                currentComponent = "tFileOutputDelimited_2";

                if (outtFileOutputDelimited_2 != null) {
                    outtFileOutputDelimited_2.flush();
                    outtFileOutputDelimited_2.close();
                }

                globalMap.put("tFileOutputDelimited_2_NB_LINE", nb_line_tFileOutputDelimited_2);
                globalMap.put("tFileOutputDelimited_2_FILE_NAME", fileName_tFileOutputDelimited_2);

                resourceMap.put("finish_tFileOutputDelimited_2", true);

                /**
                 * [tFileOutputDelimited_2 end ] stop
                 */


        } catch (java.lang.Exception e) {

            TalendException te = new TalendException(e, currentComponent, globalMap);

            throw te;
        } catch (java.lang.Error error) {
            throw error;
        } finally {

            try {

                /**
                 * [tRowGenerator_2 finally ] start
                 */

                currentComponent = "tRowGenerator_2";

                /**
                 * [tRowGenerator_2 finally ] stop
                 */

                /**
                 * [tFileOutputDelimited_2 finally ] start
                 */

                currentComponent = "tFileOutputDelimited_2";

                if (resourceMap.get("finish_tFileOutputDelimited_2") == null) {

                    java.io.Writer outtFileOutputDelimited_2 = (java.io.Writer) resourceMap.get("out_tFileOutputDelimited_2");
                    if (outtFileOutputDelimited_2 != null) {
                        outtFileOutputDelimited_2.flush();
                        outtFileOutputDelimited_2.close();
                    }

                }

                /**
                 * [tFileOutputDelimited_2 finally ] stop
                 */

            } catch (java.lang.Exception e) {
                // ignore
            } catch (java.lang.Error error) {
                // ignore
            }
            resourceMap = null;
        }

        globalMap.put("tRowGenerator_2_SUBPROCESS_STATE", 1);
    }

    private class TalendException extends Exception {

        private static final long serialVersionUID = 1L;

        private java.util.Map<String, Object> globalMap = null;

        private Exception e = null;

        private String currentComponent = null;

        private String virtualComponentName = null;

        public void setVirtualComponentName(String virtualComponentName) {
            this.virtualComponentName = virtualComponentName;
        }

        private TalendException(Exception e, String errorComponent, final java.util.Map<String, Object> globalMap) {
            this.currentComponent = errorComponent;
            this.globalMap = globalMap;
            this.e = e;
        }

        public Exception getException() {
            return this.e;
        }

        public String getCurrentComponent() {
            return this.currentComponent;
        }

        public String getExceptionCauseMessage(Exception e) {
            Throwable cause = e;
            String message = null;
            int i = 10;
            while (null != cause && 0 < i--) {
                message = cause.getMessage();
                if (null == message) {
                    cause = cause.getCause();
                } else {
                    break;
                }
            }
            if (null == message) {
                message = e.getClass().getName();
            }
            return message;
        }

        @Override
        public void printStackTrace() {
            if (!(e instanceof TalendException || e instanceof TDieException)) {
                if (virtualComponentName != null && currentComponent.indexOf(virtualComponentName + "_") == 0) {
                    globalMap.put(virtualComponentName + "_ERROR_MESSAGE", getExceptionCauseMessage(e));
                }
                globalMap.put(currentComponent + "_ERROR_MESSAGE", getExceptionCauseMessage(e));
                System.err.println("Exception in component " + currentComponent);
            }
            if (!(e instanceof TDieException)) {
                if (e instanceof TalendException) {
                    e.printStackTrace();
                } else {
                    e.printStackTrace();
                }
            }
            if (!(e instanceof TalendException)) {
                try {
                    for (java.lang.reflect.Method m : this.getClass().getEnclosingClass().getMethods()) {
                        if (m.getName().compareTo(currentComponent + "_error") == 0) {
                            break;
                        }
                    }

                    if (!(e instanceof TDieException)) {
                    }
                } catch (Exception e) {
                    this.e.printStackTrace();
                }
            }
        }
    }

    public static class row2Struct implements routines.system.IPersistableRow<row2Struct> {

        final static byte[] commonByteArrayLock_LOCAL_PROJECT_fileOutputDelimited = new byte[0];

        static byte[] commonByteArray_LOCAL_PROJECT_fileOutputDelimited = new byte[0];

        public String Column1;

        public String getColumn1() {
            return this.Column1;
        }

        public String Column2;

        public String getColumn2() {
            return this.Column2;
        }

        public String Column3;

        public String getColumn3() {
            return this.Column3;
        }

        public String Column4;

        public String getColumn4() {
            return this.Column4;
        }

        public String Column5;

        public String getColumn5() {
            return this.Column5;
        }

        public String Column6;

        public String getColumn6() {
            return this.Column6;
        }

        public String Column7;

        public String getColumn7() {
            return this.Column7;
        }

        public String Column8;

        public String getColumn8() {
            return this.Column8;
        }

        public String Column9;

        public String getColumn9() {
            return this.Column9;
        }

        public String Column10;

        public String getColumn10() {
            return this.Column10;
        }

        public String Column11;

        public String getColumn11() {
            return this.Column11;
        }

        public String Column12;

        public String getColumn12() {
            return this.Column12;
        }

        public String Column13;

        public String getColumn13() {
            return this.Column13;
        }

        public String Column14;

        public String getColumn14() {
            return this.Column14;
        }

        public String Column15;

        public String getColumn15() {
            return this.Column15;
        }

        public String Column16;

        public String getColumn16() {
            return this.Column16;
        }

        public String Column17;

        public String getColumn17() {
            return this.Column17;
        }

        public String Column18;

        public String getColumn18() {
            return this.Column18;
        }

        public String Column19;

        public String getColumn19() {
            return this.Column19;
        }

        public String Column20;

        public String getColumn20() {
            return this.Column20;
        }

        public String Column21;

        public String getColumn21() {
            return this.Column21;
        }

        public String Column22;

        public String getColumn22() {
            return this.Column22;
        }

        public String Column23;

        public String getColumn23() {
            return this.Column23;
        }

        public String Column24;

        public String getColumn24() {
            return this.Column24;
        }

        public String Column25;

        public String getColumn25() {
            return this.Column25;
        }

        public String Column26;

        public String getColumn26() {
            return this.Column26;
        }

        public String Column27;

        public String getColumn27() {
            return this.Column27;
        }

        public String Column28;

        public String getColumn28() {
            return this.Column28;
        }

        public String Column29;

        public String getColumn29() {
            return this.Column29;
        }

        public String Column30;

        public String getColumn30() {
            return this.Column30;
        }

        private String readString(ObjectInputStream dis) throws IOException {
            String strReturn = null;
            int length = 0;
            length = dis.readInt();
            if (length == -1) {
                strReturn = null;
            } else {
                if (length > commonByteArray_LOCAL_PROJECT_fileOutputDelimited.length) {
                    if (length < 1024 && commonByteArray_LOCAL_PROJECT_fileOutputDelimited.length == 0) {
                        commonByteArray_LOCAL_PROJECT_fileOutputDelimited = new byte[1024];
                    } else {
                        commonByteArray_LOCAL_PROJECT_fileOutputDelimited = new byte[2 * length];
                    }
                }
                dis.readFully(commonByteArray_LOCAL_PROJECT_fileOutputDelimited, 0, length);
                strReturn = new String(commonByteArray_LOCAL_PROJECT_fileOutputDelimited, 0, length, "UTF-8");
            }
            return strReturn;
        }

        @Override
        public void writeData(ObjectOutputStream out) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void readData(ObjectInputStream in) {
            // TODO Auto-generated method stub
            
        }
    }
}
