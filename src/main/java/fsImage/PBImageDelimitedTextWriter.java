/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fsImage;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeSymlink;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static java.util.Locale.CHINA;

/**
 * A PBImageDelimitedTextWriter generates a text representation of the PB fsimage,
 * with each element separated by a delimiter string.  All of the elements
 * common to both inodes and inodes-under-construction are included. When
 * processing an fsimage with a layout version that did not include an
 * element, such as AccessTime, the output file will include a column
 * for the value, but no value will be included.
 * <p>
 * Individual block information for each file is not currently included.
 * <p>
 * The default delimiter is tab, as this is an unlikely value to be included in
 * an inode path or other text metadata. The delimiter value can be via the
 * constructor.
 */
public class PBImageDelimitedTextWriter extends PBImageTextWriter {
    static final String DEFAULT_DELIMITER = "\t";
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm";
    private final SimpleDateFormat dateFormatter =
            new SimpleDateFormat(DATE_FORMAT);

    private final String delimiter;

    PBImageDelimitedTextWriter(String delimiter, String tempPath)
            throws IOException {
        super(tempPath);
        this.delimiter = delimiter;
    }

    private String formatDate(long date) {
        return dateFormatter.format(new Date(date));
    }

    private void append(StringBuffer buffer, int field) {
        buffer.append(delimiter);
        buffer.append(field);
    }

    private void append(StringBuffer buffer, long field) {
        buffer.append(delimiter);
        buffer.append(field);
    }

    private void append(StringBuffer buffer, String field) {
        buffer.append(delimiter);
        buffer.append(field);
    }

    @Override
    public String getEntry(String parent, INode inode) {
        JSONObject HDFSInfo = new JSONObject(true);
        String inodeName = inode.getName().toStringUtf8();
        Path path = new Path(parent.isEmpty() ? "/" : parent,
                inodeName.isEmpty() ? "/" : inodeName);
        HDFSInfo.put("InodeName",inodeName);
        HDFSInfo.put("Path", path.toString());
        HDFSInfo.put("ParentPath", parent);
        PermissionStatus p = null;
        boolean isDir = false;
        boolean hasAcl = false;
        switch (inode.getType()) {
            case FILE:
                INodeFile file = inode.getFile();
                p = getPermission(file.getPermission());
                hasAcl = file.hasAcl() && file.getAcl().getEntriesCount() > 0;
                HDFSInfo.put("Type","file");
                HDFSInfo.put("Replication", file.getReplication());
                HDFSInfo.put("ModificationTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", CHINA)
                        .format(file.getModificationTime()));
                HDFSInfo.put("AccessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", CHINA)
                        .format(file.getModificationTime()));
                HDFSInfo.put("PreferredBlockSize", file.getPreferredBlockSize());
                HDFSInfo.put("BlocksCount", file.getBlocksCount());
                HDFSInfo.put("FileSize", FSImageLoader.getFileSize(file));
                HDFSInfo.put("NSQUOTA", 0);
                HDFSInfo.put("DSQUOTA", 0);
                break;
            case DIRECTORY:
                INodeDirectory dir = inode.getDirectory();
                p = getPermission(dir.getPermission());
                hasAcl = dir.hasAcl() && dir.getAcl().getEntriesCount() > 0;
                HDFSInfo.put("Type","directory");
                HDFSInfo.put("Replication", 0);
                HDFSInfo.put("ModificationTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", CHINA)
                        .format(dir.getModificationTime()));
                HDFSInfo.put("AccessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", CHINA)
                        .format(0));
                HDFSInfo.put("PreferredBlockSize", 0);
                HDFSInfo.put("BlocksCount", 0);
                HDFSInfo.put("FileSize", 0);
                HDFSInfo.put("NSQUOTA", dir.getNsQuota());
                HDFSInfo.put("DSQUOTA", dir.getDsQuota());
                isDir = true;
                break;
            case SYMLINK:
                INodeSymlink s = inode.getSymlink();
                p = getPermission(s.getPermission());
                HDFSInfo.put("Type","symlink");
                HDFSInfo.put("Replication", 0);
                HDFSInfo.put("ModificationTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", CHINA)
                        .format(s.getModificationTime()));
                HDFSInfo.put("AccessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", CHINA)
                        .format(s.getAccessTime()));
                HDFSInfo.put("PreferredBlockSize", 0);
                HDFSInfo.put("BlocksCount", 0);
                HDFSInfo.put("FileSize", 0);
                HDFSInfo.put("NSQUOTA", 0);
                HDFSInfo.put("DSQUOTA", 0);
                break;
            default:
                break;
        }
        assert p != null;
        String dirString = isDir ? "d" : "-";
        String aclString = hasAcl ? "+" : "";
        HDFSInfo.put("Permission", dirString + p.getPermission().toString() + aclString);
        HDFSInfo.put("UserName", p.getUserName());
        HDFSInfo.put("GroupName", p.getGroupName());
        HDFSInfo.put("Area", "china1");
        HDFSInfo.put("ClusterName", "ht_skyhorse_hbase_hdfs");
        HDFSInfo.put("NameSpace", "skyhorse");
        HDFSInfo.put("Protocol", "hdfs");
        HDFSInfo.put("@timestamp", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS", CHINA).format(new Date()));
        return HDFSInfo.toString();
    }

    @Override
    public String getHeader() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("Path");
        append(buffer, "Replication");
        append(buffer, "ModificationTime");
        append(buffer, "AccessTime");
        append(buffer, "PreferredBlockSize");
        append(buffer, "BlocksCount");
        append(buffer, "FileSize");
        append(buffer, "NSQUOTA");
        append(buffer, "DSQUOTA");
        append(buffer, "Permission");
        append(buffer, "UserName");
        append(buffer, "GroupName");
        return buffer.toString();
    }
}
