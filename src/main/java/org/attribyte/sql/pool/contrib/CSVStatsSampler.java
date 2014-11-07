/*
 * Copyright 2010 Attribyte, LLC 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License.  
 * 
 */

package org.attribyte.sql.pool.contrib;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.LinkedList;
import java.util.Properties;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

import org.attribyte.api.InitializationException;
import org.attribyte.api.Logger;
import org.attribyte.sql.pool.ConnectionPool;
import org.attribyte.sql.pool.ConnectionPool.Stats;
import org.attribyte.util.StringUtil;

/**
 * A sampler that writes pool statistics to a CSV file.
 */
public class CSVStatsSampler implements ConnectionPool.StatsSampler {

   public static final class Record {

      public Record(String string) {
         String[] vals = string.split(",");
         int index = 0;
         sequence = vals[index++];
         acquisitions = vals[index++];
         acquisitionsPerSecond = vals[index++];
         failedAcquisitions = vals[index++];
         failedConnections = vals[index++];
         forcedClose = vals[index++];
         segmentExpansions = vals[index++];
         activeUnmanagedConnectionCount = vals[index++];
         activeSegments = vals[index++];
         cumulativeAcquisitions = vals[index++];
         cumulativeFailedAcquisitions = vals[index++];
         cumulativeFailedConnections = vals[index++];
         cumulativeFailedClose = vals[index++];
         time = vals[index];
      }

      public long getTimestamp() {
         SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
         try {
            return dateFormat.parse(time).getTime();
         } catch(Exception e) {
            return 0L;
         }
      }

      public final String sequence;
      public final String acquisitions;
      public final String acquisitionsPerSecond;
      public final String failedAcquisitions;
      public final String failedConnections;
      public final String forcedClose;
      public final String segmentExpansions;
      public final String activeUnmanagedConnectionCount;
      public final String activeSegments;
      public final String cumulativeAcquisitions;
      public final String cumulativeFailedAcquisitions;
      public final String cumulativeFailedConnections;
      public final String cumulativeFailedClose;
      public final String time;
   }

   private Stats lastStats = null;
   private long frequencyMillis;
   private File file;
   private int index;
   private NumberFormat decFormat;
   private SimpleDateFormat dateFormat;
   private SimpleDateFormat fileDateFormat;
   private String fileRoot;
   private String fileExtension;
   private boolean roll = false;
   private final List<Record> currentRecords = new LinkedList<Record>();
   private String currDayFile;
   private Logger logger;
   private final String LINE_SEP;


   /**
    * Creates a sampler.
    */
   public CSVStatsSampler() {
      decFormat = NumberFormat.getNumberInstance();
      decFormat.setMinimumFractionDigits(2);
      decFormat.setMaximumFractionDigits(2);
      dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
      fileDateFormat = new SimpleDateFormat("yyyy-MM-dd");
      LINE_SEP = System.getProperty("line.separator");
   }

   /**
    * Accept a statistics sample.
    * @param stats The statistics.
    */
   public void acceptSample(Stats stats) {
      if(lastStats != null) {
         boolean rolled = rollFile();
         if(rolled) {
            writeHeader();
            currentRecords.clear();
         }
         writeStats(stats);
      }
      lastStats = stats;
      index++;
   }

   public void init(final Properties props, final Logger logger) throws InitializationException {

      this.logger = logger;

      String frequencyStr = props.getProperty("frequency");
      char unit = frequencyStr.charAt(frequencyStr.length() - 1);
      switch(unit) {
         case 's':
            frequencyMillis = Long.parseLong(frequencyStr.substring(0, frequencyStr.length() - 1)) * 1000L;
            break;
         case 'm':
            frequencyMillis = Long.parseLong(frequencyStr.substring(0, frequencyStr.length() - 1)) * 6000L;
            break;
         default:
            frequencyMillis = Long.parseLong(frequencyStr);
      }

      String file = props.getProperty("file");
      if(!StringUtil.hasContent(file)) {
         throw new InitializationException("CSVStatsSampler: A 'file' must be specified");
      }

      this.file = new File(file);

      this.roll = props.getProperty("roll", "false").equalsIgnoreCase("true");

      int index = file.lastIndexOf('.');
      if(index > 0) {
         this.fileExtension = file.substring(index);
         this.fileRoot = file.substring(0, index);
      }

      currDayFile = buildCurrDayFile();

      String append = props.getProperty("append");
      if(append != null && append.equalsIgnoreCase("true")) {
         if(this.file.exists()) {
            index = nextIndex();
         }
      } else {
         if(this.file.exists()) {
            this.file.delete();
         }
         writeHeader();
      }
   }

   public long getFrequencyMillis() {
      return frequencyMillis;
   }

   public List<Record> readRecords(Date endDate, int numDays) throws IOException {

      List<Record> recordList = new LinkedList<Record>();
      Date currDate = new Date(endDate.getTime() - (24L * 3600L * 1000L * numDays));
      for(int i = 0; i < numDays; i++) {
         List<Record> currList = readRecords(currDate);
         recordList.addAll(currList);
         currDate = new Date(currDate.getTime() + (24L * 3600L * 1000L));
      }

      return recordList;
   }

   public List<Record> readRecords(Date date) throws IOException {

      String filename = buildDayFile(date);
      if(filename.equals(currDayFile)) {
         return currentRecords;
      }

      File file = new File(filename);
      if(!file.exists()) {
         return Collections.emptyList();
      } else {
         List<Record> recordList = new LinkedList<Record>();
         FileReader fr = new FileReader(file);
         BufferedReader reader = new BufferedReader(fr);
         try {
            String currLine = reader.readLine(); //Header
            while((currLine = reader.readLine()) != null) {
               recordList.add(new Record(currLine));
            }
         } finally {
            fr.close();
         }

         return recordList;
      }
   }

   /**
    * Gets the "per" rate unit for acquisition rates.
    * @param unit The unit.
    * @return The rate unit description.
    */
   private static String getRateUnit(final TimeUnit unit) {
      switch(unit) {
         case MINUTES: return "/Minute";
         case SECONDS: return "/Second";
         case HOURS: return " /Hour";
         case DAYS: return "/Day";
         case MILLISECONDS: return "/Millisecond";
         case MICROSECONDS: return "/Microsecond";
         default: return "";
      }
   }

   private void writeHeader() {

      StringBuilder buf = new StringBuilder();
      buf.append("\"Sequence\"").append(',');
      buf.append("\"Acquisition Rate Unit\"").append(',');

      buf.append("\"Acquisitions\"").append(',');
      buf.append("\"Acquisition Rate\"").append(',');
      buf.append("\"1m Avg Acquisition Rate\"").append(',');
      buf.append("\"5m Avg Acquisition Rate\"").append(',');
      buf.append("\"15m Avg Acquisition Rate\"").append(',');

      buf.append("\"Active Connections Used\"").append(',');
      buf.append("\"Available Connections Used\"").append(',');

      buf.append("\"Failed Acquisitions\"").append(',');
      buf.append("\"Failed Acquisition Rate\"").append(',');
      buf.append("\"1m Avg Failed Acquisition Rate\"").append(',');
      buf.append("\"5m Avg Failed Acquisition Rate\"").append(',');
      buf.append("\"15m Avg Failed AcquisitionRate \"").append(',');

      buf.append("\"Failed Connections\"").append(',');
      buf.append("\"Forced Close\"").append(',');
      buf.append("\"Segment Expansions\"").append(',');
      buf.append("\"Active Unmanaged Connections\"").append(',');
      buf.append("\"Active Segments\"").append(',');
      buf.append("\"Cumulative Acquisitions\"").append(',');
      buf.append("\"Cumulative Failed Acquisitions\"").append(',');
      buf.append("\"Cumulative Failed Connections\"").append(',');
      buf.append("\"Cumulative Forced Close\"").append(',');
      buf.append("\"Time\"");
      buf.append(LINE_SEP);

      FileWriter fw = null;
      try {
         fw = new FileWriter(file, true);
         fw.write(buf.toString());
         fw.flush();
      } catch(IOException ioe) {
         if(logger != null) {
            logger.error("CSVStatsSampler: Unable to log", ioe);
         }
      } finally {
         if(fw != null) {
            try {
               fw.close();
            } catch(IOException ioe) {
               //Ignore
            }
         }
      }
   }

   private void writeStats(Stats stats) {

      Stats delta = stats.subtract(lastStats);

      long acquisitions = delta.getConnectionCount();
      double acquisitionRate = (double)acquisitions / (double)(TimeUnit.MILLISECONDS.convert(frequencyMillis, stats.getAcquisitionRateUnit()));
      long failedAcquisitions = delta.getFailedAcquisitionCount();
      double failedAcquisitionRate = (double)failedAcquisitions / (double)(TimeUnit.MILLISECONDS.convert(frequencyMillis, stats.getAcquisitionRateUnit()));

      long failedConnections = delta.getFailedConnectionErrorCount();
      long activeTimeoutCount = delta.getActiveTimeoutCount();
      long segmentExpansions = delta.getSegmentExpansionCount();
      long activeUnmanagedConnections = delta.getActiveUnmanagedConnectionCount();
      int activeSegments = stats.getActiveSegments();
      long cumulativeAcquisitions = stats.getConnectionCount();
      long cumulativeFailedAcquisitions = stats.getFailedAcquisitionCount();
      long cumulativeFailedConnections = stats.getFailedConnectionErrorCount();
      long cumulativeActiveTimeoutCount = stats.getActiveTimeoutCount();


      StringBuilder buf = new StringBuilder();
      buf.append(index).append(',');
      buf.append(getRateUnit(stats.getAcquisitionRateUnit()));

      buf.append(acquisitions).append(',');
      buf.append(decFormat.format(acquisitionRate)).append(',');
      buf.append(decFormat.format(stats.getOneMinuteAcquisitionRate())).append(',');
      buf.append(decFormat.format(stats.getFiveMinuteAcquisitionRate())).append(',');
      buf.append(decFormat.format(stats.getFifteenMinuteAcquisitionRate())).append(',');

      buf.append(decFormat.format(stats.getActiveConnectionUtilization())).append(',');
      buf.append(decFormat.format(stats.getAvailableConnectionUtilization())).append(',');


      buf.append(failedAcquisitions).append(',');
      buf.append(decFormat.format(failedAcquisitionRate)).append(',');
      buf.append(decFormat.format(stats.getOneMinuteFailedAcquisitionRate())).append(',');
      buf.append(decFormat.format(stats.getFiveMinuteFailedAcquisitionRate())).append(',');
      buf.append(decFormat.format(stats.getFifteenMinuteFailedAcquisitionRate())).append(',');

      buf.append(failedConnections).append(',');
      buf.append(activeTimeoutCount).append(',');
      buf.append(segmentExpansions).append(',');
      buf.append(activeUnmanagedConnections).append(',');
      buf.append(activeSegments).append(',');
      buf.append(cumulativeAcquisitions).append(',');
      buf.append(cumulativeFailedAcquisitions).append(',');
      buf.append(cumulativeFailedConnections).append(',');
      buf.append(cumulativeActiveTimeoutCount).append(',');
      buf.append(dateFormat.format(new Date()));

      if(roll) {
         currentRecords.add(new Record(buf.toString()));
      }
      buf.append(LINE_SEP);

      FileWriter fw = null;
      try {
         fw = new FileWriter(file, true);
         fw.write(buf.toString());
         fw.flush();
      } catch(IOException ioe) {
         if(logger != null) {
            logger.error("CSVStatsSampler: Unable to log", ioe);
         }
      } finally {
         if(fw != null) {
            try {
               fw.close();
            } catch(IOException ioe) {
               //Ignore
            }
         }
      }
   }

   /**
    * Gets the next index in the CSV file.
    * @return The next index.
    * @throws InitializationException if the file could not be read.
    */
   private int nextIndex() throws InitializationException {
      FileReader fr = null;
      try {
         fr = new FileReader(file);
         BufferedReader reader = new BufferedReader(fr);
         String currLine;
         String lastLine = null;
         while((currLine = reader.readLine()) != null) {
            lastLine = currLine;
         }

         if(lastLine == null) {
            return 0;
         } else {
            int index = lastLine.indexOf(',');
            if(index < 1) {
               return 0;
            } else {
               return Integer.parseInt(lastLine.substring(0, index));
            }
         }
      } catch(IOException ioe) {
         throw new InitializationException("Unable to read " + file.getAbsolutePath(), ioe);
      } finally {
         try {
            if(fr != null) {
               fr.close();
            }
         } catch(IOException ioe) {
            //Ignore
         }
      }
   }

   private boolean rollFile() {

      if(!roll) {
         return false;
      }

      try {
         String currDayFile = buildCurrDayFile();
         if(!this.currDayFile.equals(currDayFile)) {
            boolean renamed = this.file.renameTo(new File(this.currDayFile));
            if(renamed) {
               this.file.delete();
               this.file.createNewFile();
               this.currDayFile = currDayFile;
               return true;
            }
         }
         return false;
      } catch(IOException ioe) {
         if(logger != null) {
            logger.error("CSVStatsSampler: Unable to roll", ioe);
         }
         return false;
      }
   }

   /**
    * Builds a file of the format [path]/[file]-YYYY-mm-dd.[ext].
    * @return The file.
    */
   private String buildCurrDayFile() {
      return buildDayFile(new Date());
   }

   /**
    * Builds a file of the format [path]/[file]-YYYY-mm-dd.[ext].
    * @param date The date.
    * @return The file.
    */
   private String buildDayFile(Date date) {
      StringBuilder buf = new StringBuilder(fileRoot);
      buf.append("_");
      buf.append(fileDateFormat.format(date));
      buf.append(fileExtension);
      return buf.toString();
   }
}   
