package sics.seiois.mlsserver.biz.der.metanome.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.*;
import java.io.File;

public class Dir {
    private Collection<String> relation_names;
    private Collection<String> absolute_paths;
    private String path;
    private Map<String,String> paths;

    public Dir(String _path) {
        path = _path;

        final File folder = new File(path);
        relation_names = new ArrayList<>();
        absolute_paths = new ArrayList<>();

        search(".*\\.csv", folder);

    }

    public Dir(Map<String,String> _path, double ratio) {
        try {
            paths = _path;
            FileSystem hdfs = FileSystem.get(new Configuration());

            relation_names = new ArrayList<>();
            absolute_paths = new ArrayList<>();

            for (Map.Entry<String,String> entry : paths.entrySet()) {
                RemoteIterator<LocatedFileStatus> listFiles = hdfs.listFiles(new Path(entry.getValue()), true);
                while (listFiles.hasNext()) {
                    LocatedFileStatus f = listFiles.next();
                    if (f.isDirectory()) {
                        continue;
                    }
                    if (f.isFile()) {
                        if (f.getPath().getName().matches(".*\\.csv")) {
                            relation_names.add(entry.getKey());
                            absolute_paths.add(f.getPath().toString());
                        }
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        int relation_num = (int)(relation_names.size() * ratio);
        if (relation_num == 0) {
            relation_num = 1;
        }

        Collection<String> relation_names_ = new ArrayList<>();
        Collection<String> absolute_paths_ = new ArrayList<>();

        Iterator<String> iter_r = relation_names.iterator();
        Iterator<String> iter_a = absolute_paths.iterator();

        int count = 0;
        while (iter_r.hasNext()) {
            if (count >= relation_num) {
                break;
            }
            String r = iter_r.next();
            relation_names_.add(r);
            String a = iter_a.next();
            absolute_paths_.add(a);
            count ++;
        }

        relation_names = relation_names_;
        absolute_paths = absolute_paths_;
    }

//    public Dir(String _path, double ratio) {
//        try {
//            path = _path;
//            FileSystem hdfs = FileSystem.get(new Configuration());
//
//            relation_names = new ArrayList<>();
//            absolute_paths = new ArrayList<>();
//
//            RemoteIterator<LocatedFileStatus> listFiles = hdfs.listFiles(new Path(path), true);
//            while (listFiles.hasNext()) {
//                LocatedFileStatus f = listFiles.next();
//                if (f.isDirectory())
//                    continue;
//                if (f.isFile()) {
//                    if (f.getPath().getName().matches(".*\\.csv")) {
//                        relation_names.add(f.getPath().getName().substring(0, f.getPath().getName().length() - 4));
//                        absolute_paths.add(f.getPath().toString());
//                    }
//                }
//            }
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }
//
//        int relation_num = (int)(relation_names.size() * ratio);
//        if (relation_num == 0) relation_num = 1;
//
//        Collection<String> relation_names_ = new ArrayList<>();
//        Collection<String> absolute_paths_ = new ArrayList<>();
//
//        Iterator<String> iter_r = relation_names.iterator();
//        Iterator<String> iter_a = absolute_paths.iterator();
//
//        int count = 0;
//        while (iter_r.hasNext()) {
//            if (count >= relation_num)
//                break;
//            String r = iter_r.next();
//            relation_names_.add(r);
//            String a = iter_a.next();
//            absolute_paths_.add(a);
//            count ++;
//        }
//
//        relation_names = relation_names_;
//        absolute_paths = absolute_paths_;
//    }


    public Dir(String _path, double ratio) {
        path = _path;

        final File folder = new File(path);
        relation_names = new ArrayList<>();
        absolute_paths = new ArrayList<>();

        search(".*\\.csv", folder);

        int relation_num = (int)(relation_names.size() * ratio);
        if (relation_num == 0) {
            relation_num = 1;
        }

        Collection<String> relation_names_ = new ArrayList<>();
        Collection<String> absolute_paths_ = new ArrayList<>();

        Iterator<String> iter_r = relation_names.iterator();
        Iterator<String> iter_a = absolute_paths.iterator();

        int count = 0;
        while (iter_r.hasNext()) {
            if (count >= relation_num) {
                break;
            }
            String r = iter_r.next();
            relation_names_.add(r);
            String a = iter_a.next();
            absolute_paths_.add(a);
            count ++;
        }

        relation_names = relation_names_;
        absolute_paths = absolute_paths_;
    }

    public void search(final String pattern, final File folder) {
        for (final File f : folder.listFiles()) {
            if (f.isDirectory()) {
                continue;
            }
            if (f.isFile()) {
                if (f.getName().matches(pattern)) {
                    relation_names.add(f.getName().substring(0, f.getName().length() - 4));
                    absolute_paths.add(f.getAbsolutePath());
                }
            }
        }
    }

    public Iterator<String> iterator_r() {
        return relation_names.iterator();
    }

    public Iterator<String> iterator_a() {
        return absolute_paths.iterator();
    }

}
