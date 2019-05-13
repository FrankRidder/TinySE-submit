package edu.hanyang.submit;

import java.io.*;
import java.util.*;

import edu.hanyang.utils.DiskIO;
import edu.hanyang.indexer.ExternalSort;

import org.apache.commons.lang3.tuple.MutableTriple;


public class TinySEExternalSort implements ExternalSort {

    private int NFILES;
    private int NBLOCKS;
    private int BLOCKSIZE;

    public void sort(String infile, String outfile, String tmpDir, int blocksize, int nblocks) throws IOException {

        int dataPerRun = (((blocksize / Integer.SIZE) * (nblocks - 2)) / 3);

        int maxBitsPerRun = dataPerRun / 8;

        NBLOCKS = nblocks;
        BLOCKSIZE = blocksize;
        NFILES = 1;

        DataInputStream is = DiskIO.open_input_run(infile, BLOCKSIZE);

        ArrayList<MutableTriple<Integer, Integer, Integer>> data = new ArrayList<>();
        MutableTriple<Integer, Integer, Integer> tmp = new MutableTriple<>();


        while (is.available() != 0) {
            if ((is.available() / (3 * Integer.SIZE)) > maxBitsPerRun) {
//                DiskIO.read_array(is, dataPerRun, data);// java.lang.IndexOutOfBoundsException: Index 0 out of bounds for length 0
                for (int i = 0; i < dataPerRun; i++) {
                    tmp.setLeft(is.readInt());
                    tmp.setMiddle(is.readInt());
                    tmp.setRight(is.readInt());

                    data.add(tmp);
                }
            } else {
                //               DiskIO.read_array(is, (is.available() / (3 * Integer.SIZE)), data);// java.lang.IndexOutOfBoundsException: Index 0 out of bounds for length 0
                while (is.available() > 0) {
                    tmp.setLeft(is.readInt());
                    tmp.setMiddle(is.readInt());
                    tmp.setRight(is.readInt());

                    data.add(tmp);
                }
            }

            data.sort(new TripleComp());

            File file = new File(tmpDir + "/" + "tmp" + NFILES + ".data");

            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdir();
            }

            file.createNewFile();

            DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file), BLOCKSIZE));

            //DiskIO.append_arr(os, data, data.size());
            for (MutableTriple<Integer, Integer, Integer> datum : data) {
                os.writeInt(datum.getLeft());
                os.writeInt(datum.getMiddle());
                os.writeInt(datum.getRight());
            }

            os.flush();

            data.clear();
            NFILES++;
            os.close();
        }

        _externalMergeSort(tmpDir, outfile);
    }


    private void _externalMergeSort(String tmpDir, String outputFile) throws IOException {
        File[] fileArr = (new File(tmpDir + "/")).listFiles();
        ArrayList<DataInputStream> files = new ArrayList<>();

        if (fileArr.length < (NBLOCKS - 1)) {
            for (File f : fileArr) {
                files.add(DiskIO.open_input_run(f.getAbsolutePath(), BLOCKSIZE));
            }

            _mergeSort(files, outputFile);

            for (File f : fileArr) {
                f.delete();
            }
            files.clear();

        } else {
            for (File f : fileArr) {

                files.add(DiskIO.open_input_run(f.getAbsolutePath(), BLOCKSIZE));

                if (files.size() == (NBLOCKS - 1)) {
                    String tmpfile = (tmpDir + "/" + "tmp" + NFILES + ".data");

                    NFILES++;
                    _mergeSort(files, tmpfile);
                    files.clear();

                }
            }
            if (files.size() != 0) {
                String tmpfile = (tmpDir + "/" + "tmp" + NFILES + ".data");

                NFILES++;
                _mergeSort(files, tmpfile);
                files.clear();
            }
            for (File f : fileArr) {
                f.delete();
            }
            files.clear();
            _externalMergeSort(tmpDir, outputFile);
        }


    }

    private void _mergeSort(ArrayList<DataInputStream> files, String outputString) throws IOException {
        PriorityQueue<DataManager> queue = new PriorityQueue<>();

        ArrayList<MutableTriple<Integer, Integer, Integer>> output = new ArrayList<>();

        DataManager dm;

        File outputFile = new File(outputString);

        outputFile.createNewFile();

        DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile, true), BLOCKSIZE));

        for (DataInputStream file : files) {
            queue.add(new DataManager(file));
        }
        dm = queue.poll();
        while (queue.size() != 0) {
            if (dm.isEmpty()) {
                dm = queue.poll();
            }

            output.add(dm.getTuple());
            if (output.size() == files.size()) {
                output.sort(new TripleComp());

                os.writeInt(output.get(0).getLeft());
                os.writeInt(output.get(0).getMiddle());
                os.writeInt(output.get(0).getRight());

                os.flush();
                output.remove(0);
            }

        }
        if (output.size() > 0) {
            output.sort(new TripleComp());
            for (MutableTriple<Integer, Integer, Integer> MutableTriple : output) {
                os.writeInt(MutableTriple.getLeft());
                os.writeInt(MutableTriple.getMiddle());
                os.writeInt(MutableTriple.getRight());
            }
            os.flush();
        }
        os.close();
    }

    private class TripleComp implements Comparator<MutableTriple<Integer, Integer, Integer>> {

        @Override
        public int compare(MutableTriple<Integer, Integer, Integer> o1, MutableTriple<Integer, Integer, Integer> o2) {
            return o1.compareTo(o2);
        }
    }


    private class DataManager implements Comparable<DataManager> {
        DataInputStream input;
        private MutableTriple<Integer, Integer, Integer> triple;

        private DataManager(DataInputStream is) throws IOException {
            this.input = is;
            this.triple = new MutableTriple<>();

            if (this.input.available() > 0) {
                this.triple.setLeft(this.input.readInt());
                this.triple.setMiddle(this.input.readInt());
                this.triple.setRight(this.input.readInt());
            } else {
                this.triple = null;
            }
        }

        @Override
        public int compareTo(DataManager m2) {
            return this.triple.compareTo(m2.triple);
        }

        private MutableTriple<Integer, Integer, Integer> getTuple() throws IOException {
            MutableTriple<Integer, Integer, Integer> temp = this.triple;
            getNext();
            return temp;
        }

        private boolean isEmpty() {
            return this.triple == null;
        }

        private void getNext() throws IOException {
            if (this.input.available() > 0) {
                this.triple.setLeft(this.input.readInt());
                this.triple.setMiddle(this.input.readInt());
                this.triple.setRight(this.input.readInt());
            } else {
                this.triple = null;
            }
        }
    }
}

