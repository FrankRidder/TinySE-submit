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
    private int BUFFERSIZE = 1024;

    public void sort(String infile, String outfile, String tmpDir, int blocksize, int nblocks) throws IOException {

        BLOCKSIZE = blocksize;
        NBLOCKS = nblocks;

        int nElement = (((BLOCKSIZE / Integer.SIZE) * (NBLOCKS - 2)) / 3) / 8;
        ArrayList<MutableTriple<Integer, Integer, Integer>> data = new ArrayList<>();

        DataInputStream input = DiskIO.open_input_run(infile, BLOCKSIZE);

        while (input.available() > 0) {
            for (int i = 0; i < nElement; i++) {
                MutableTriple<Integer, Integer, Integer> tmp = new MutableTriple<>();
                try {
                    tmp.setLeft(input.readInt());
                    tmp.setMiddle(input.readInt());
                    tmp.setRight(input.readInt());
                    data.add(tmp);
                } catch (EOFException e) {
                    break;
                }
            }
            Collections.sort(data);

            String tmpfile = tmpDir + "/run_" + 0 + "/" + NFILES + ".data";

            File file = new File(tmpDir);

            if (!file.exists()) {
                file.mkdir();
            }

            file = new File(tmpfile);

            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdir();
            }

            file.createNewFile();

            DataOutputStream output = DiskIO.open_output_run(file.getAbsolutePath(), BLOCKSIZE);
            NFILES++;

            for (MutableTriple<Integer, Integer, Integer> temp : data) {
                output.writeInt(temp.getLeft());
                output.writeInt(temp.getMiddle());
                output.writeInt(temp.getRight());
            }
            data.clear();
            output.close();


        }

        input.close();

        _externalMergeSort(tmpDir, outfile, 1);
    }


    private void _externalMergeSort(String tmpDir, String outputFile, int step) throws IOException {
        String prev_run = tmpDir + "/run_" + (step - 1);
        File[] fileArr = new File(prev_run).listFiles();
        ArrayList<DataInputStream> files = new ArrayList<>();
        NFILES = 0;

        int n_way_merge = NBLOCKS > 64 ? 64 : NBLOCKS;
        if (fileArr.length <= n_way_merge) {

            for (File f : fileArr) {
                DataInputStream dos = DiskIO.open_input_run(f.getAbsolutePath(), BUFFERSIZE);
                files.add(dos);
            }
            _mergeSort(files, outputFile);
            for (File f : fileArr) {
                f.delete();
            }

        } else {

            for (File f : fileArr) {
                DataInputStream dos = DiskIO.open_input_run(f.getAbsolutePath(), BUFFERSIZE);
                files.add(dos);

                if (files.size() == n_way_merge) {
                    String tmpfile = tmpDir + "/run_" + step + "/" + NFILES + ".data";
                    File file = new File(tmpfile);
                    if (!file.getParentFile().exists()) {
                        file.getParentFile().mkdir();
                    }
                    file.createNewFile();
                    NFILES++;
                    _mergeSort(files, file.getAbsolutePath());

                    files.clear();

                }
            }
            if (files.size() > 0) {
                String tmpfile = tmpDir + "/run_" + step + "/" + NFILES + ".data";
                File file = new File(tmpfile);
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdir();
                }
                file.createNewFile();
                NFILES++;
                _mergeSort(files, file.getAbsolutePath());
                files.clear();
            }
            for (File f : fileArr) {
                f.delete();
            }

            _externalMergeSort(tmpDir, outputFile, step + 1);
        }

    }

    private void _mergeSort(ArrayList<DataInputStream> files, String outputString) throws IOException {
        DataOutputStream output = DiskIO.open_output_run(outputString, BUFFERSIZE);

        PriorityQueue<DataManager> queue = new PriorityQueue<>(new DataComp());

        for (DataInputStream f : files) {
            try {
                DataManager dm = new DataManager(f.readInt(), f.readInt(), f.readInt(), files.indexOf(f));
                queue.add(dm);

            } catch (EOFException e) {
                continue;
            }
        }

        while (queue.size() != 0) {
            try {
                DataManager dm = queue.poll();
                MutableTriple<Integer, Integer, Integer> tmp = dm.getTuple();

                output.writeInt(tmp.getLeft());
                output.writeInt(tmp.getMiddle());
                output.writeInt(tmp.getRight());

                output.flush();

                dm.setTuple(files.get(dm.index).readInt(), files.get(dm.index).readInt(), files.get(dm.index).readInt());

                queue.add(dm);

            } catch (EOFException e) {
                continue;
            }
        }
        output.close();
    }

    private class DataComp implements Comparator<DataManager> {

        @Override
        public int compare(DataManager o1, DataManager o2) {
            return o1.tuple.compareTo(o2.tuple);
        }
    }

    private class DataManager {

        MutableTriple<Integer, Integer, Integer> tuple;
        int index;

        private DataManager(int left, int middle, int right, int index) {
            tuple = new MutableTriple<>(left, middle, right);
            this.index = index;
        }

        private void setTuple(int left, int middle, int right) {
            tuple.setLeft(left);
            tuple.setMiddle(middle);
            tuple.setRight(right);
        }

        private MutableTriple<Integer, Integer, Integer> getTuple() {
            return this.tuple;
        }
    }
}

