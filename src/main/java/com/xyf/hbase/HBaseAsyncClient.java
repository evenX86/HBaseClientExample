package com.xyf.hbase;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * AsyncHBase Client
 *
 * Source code : https://github.com/hbaseinaction/twitbase-async/blob/master/src/main/java/HBaseIA/TwitBase/AsyncUsersTool.java
 *
 */
public class HBaseAsyncClient {

    static final byte[] TABLE_NAME = "users".getBytes();
    static final byte[] INFO_FAM = "info".getBytes();
    static final byte[] PASSWORD_COL = "password".getBytes();
    static final byte[] NAME_COL = "name".getBytes();
    static final byte[] EMAIL_COL = "email".getBytes();
    static final Boolean BATCH_PUT = true;

    public static final String usage =
            "usertool action ...\n" +
                    "  help - print this message and exit.\n" +
                    "  update - update passwords for all installed users.\n";

    static byte[] mkNewPassword(byte[] seed) {
        UUID u = UUID.randomUUID();
        return u.toString().replace("-", "").toLowerCase().getBytes();
    }

    static void latency() throws Exception {
        if (System.currentTimeMillis() % 3 == 0) {
            LOG.info("a thread is napping...");
            Thread.sleep(1000);
        }
    }

    static boolean entropy(Boolean val) {
        if (System.currentTimeMillis() % 5 == 0) {
            LOG.info("entropy strikes!");
            return false;
        }
        return (val == null) ? Boolean.TRUE : val;
    }

    static final class UpdateResult {
        public String userId;
        public boolean success;
    }

    @SuppressWarnings("serial")
    static final class UpdateFailedException extends Exception {
        public UpdateResult result;

        public UpdateFailedException(UpdateResult r) {
            this.result = r;
        }
    }

    @SuppressWarnings("serial")
    static final class SendMessageFailedException extends Exception {
        public SendMessageFailedException() {
            super("Failed to send message!");
        }
    }

    /**
     * 等待HBase操作结果返回Response
     */
    static final class InterpretResponse
            implements Callback<UpdateResult, Boolean> {

        private String userId;

        InterpretResponse(String userId) {
            this.userId = userId;
        }

        public UpdateResult call(Boolean response) throws Exception {
            latency();

            UpdateResult r = new UpdateResult();
            r.userId = this.userId;
            r.success = entropy(response);
            if (!r.success)
                throw new UpdateFailedException(r);

            latency();
            return r;
        }

        @Override
        public String toString() {
            return String.format("InterpretResponse<%s>", userId);
        }
    }

    /**
     * 密码更新成功
     */
    static final class ResultToMessage
            implements Callback<String, UpdateResult> {

        public String call(UpdateResult r) throws Exception {
            latency();
            String fmt = "password change for user %s successful.";
            latency();
            return String.format(fmt, r.userId);
        }

        @Override
        public String toString() {
            return "ResultToMessage";
        }
    }

    /**
     * 密码更新失败
     */
    static final class FailureToMessage
            implements Callback<String, UpdateFailedException> {

        public String call(UpdateFailedException e) throws Exception {
            latency();
            String fmt = "%s, your password is unchanged!";
            latency();
            return String.format(fmt, e.result.userId);
        }

        @Override
        public String toString() {
            return "FailureToMessage";
        }
    }

    /**
     * 发送信息
     */
    static final class SendMessage
            implements Callback<Boolean, String> {

        public Boolean call(String s) throws Exception {
            latency();
            if (entropy(null))
                throw new SendMessageFailedException();
            LOG.info(s);
            latency();
            return Boolean.TRUE;
        }

        @Override
        public String toString() {
            return "SendMessage";
        }
    }

    /**
     * @param client
     * @return
     * @throws Throwable
     */
    static List<Deferred<Boolean>> doList(HBaseClient client)
            throws Throwable {
        final Scanner scanner = client.newScanner(TABLE_NAME);
        scanner.setFamily(INFO_FAM);
        scanner.setQualifier(PASSWORD_COL);


        ArrayList<ArrayList<KeyValue>> rows = null;
        ArrayList<Deferred<Boolean>> workers = new ArrayList<>();
        //线程阻塞到直到所有的查询都返回结果
        while ((rows = scanner.nextRows(1).joinUninterruptibly()) != null) {
            LOG.info("received a page of users.");
            for (ArrayList<KeyValue> row : rows) {
                KeyValue kv = row.get(0);
                byte[] expected = kv.value();
                String userId = new String(kv.key());
                PutRequest put = new PutRequest(
                        TABLE_NAME, kv.key(), kv.family(),
                        kv.qualifier(), mkNewPassword(expected));
                //任务链构建
                Deferred<Boolean> d = client.compareAndSet(put, expected)
                        .addCallback(new InterpretResponse(userId))
                        .addCallbacks(new ResultToMessage(), new FailureToMessage())
                        .addCallback(new SendMessage());
                workers.add(d);
            }
        }
        return workers;
    }

    /**
     * @param client
     * @return
     * @throws Throwable
     */
    static List<Deferred<Object>> insertList(HBaseClient client, List<PutRequest> puts)
            throws Throwable {
        ArrayList<Deferred<Object>> workers = new ArrayList<>();
        LOG.info("received a page of users.");
        for (PutRequest put : puts) {
            //任务链构建
            Deferred<Object> d = client.put(put);
            workers.add(d);
        }
        return workers;
    }

    public static void main(String[] args) throws Throwable {
        if (args.length == 0 || "help".equals(args[0])) {
            System.out.println(usage);
            System.exit(0);
        }

        final HBaseClient client = new HBaseClient("dev-test");

        if ("update".equals(args[0])) {
            Deferred<ArrayList<Boolean>> d = Deferred.group(doList(client));
            try {
                d.join();
            } catch (DeferredGroupException e) {
                LOG.info(e.getCause().getMessage());
            }
        } else if ("insert".equals(args[0])) {
            List<PutRequest> puts = new ArrayList<>();
            for (int i=0;i<100;i++) {
                PutRequest put = new PutRequest(TABLE_NAME,("0"+i).getBytes(),INFO_FAM,PASSWORD_COL,"123".getBytes());
                PutRequest put1 = new PutRequest(TABLE_NAME,("0"+i).getBytes(),INFO_FAM,NAME_COL,"liwei".getBytes());
                puts.add(put);
                puts.add(put1);
            }
            Deferred<ArrayList<Object>> d = Deferred.group(insertList(client,puts));
            try {
                d.join();
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        //线程阻塞直到shutdown完成
        client.shutdown().joinUninterruptibly();
    }

    static final Logger LOG = LoggerFactory.getLogger(HBaseAsyncClient.class);
}
