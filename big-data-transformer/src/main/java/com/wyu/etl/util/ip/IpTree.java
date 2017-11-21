package com.wyu.etl.util.ip;

/**
 * 采用二叉树建立表格中的数据
 */
public class IpTree {

    private static IpTree instance = null;
    private final String NO_ADDRESS = "未知";
    private IpNode rootNode = new IpNode();

    private IpTree() {

    }

    public static IpTree getInstance() {
        if (instance == null) {
            instance = new IpTree();
        }
        return instance;
    }

    /**
     * 具体构建二叉树的方法
     *
     * @param ipStart
     * @param ipEnd
     * @param addressCode province+","+city
     */
    public void train(String ipStart, String ipEnd, String addressCode) {

        int ipS = ipToInt(ipStart);
        int ipE = ipToInt(ipEnd);

        if (ipE == -1 || ipS == -1) {
            return;
        }

        IpNode curNode = rootNode;
        IpNode leftNode = null;
        IpNode rightNode = null;
        boolean flag = false;

        for (int i = 0; i < 32; i++) {

            int ipSBit = (0x80000000 & ipS) >>> 31;
            int ipEBit = (0x80000000 & ipE) >>> 31;

            if (flag == false) {

                if ((ipSBit ^ ipEBit) == 0) {

                    if (ipSBit == 1) {
                        if (curNode.rightNode == null) {
                            curNode.rightNode = new IpNode();
                        }
                        curNode = curNode.rightNode;
                    } else {
                        if (curNode.leftNode == null) {
                            curNode.leftNode = new IpNode();
                        }
                        curNode = curNode.leftNode;
                    }
                    if (i == 31) {
                        curNode.addressCode = addressCode;
                    }

                } else {
                    flag = true;
                    if (curNode.leftNode == null) {
                        curNode.leftNode = new IpNode();
                    }
                    leftNode = curNode.leftNode;

                    if (curNode.rightNode == null) {
                        curNode.rightNode = new IpNode();
                    }

                    rightNode = curNode.rightNode;

                    if (i == 31) {
                        leftNode.addressCode = addressCode;
                        rightNode.addressCode = addressCode;
                    }
                }
            } else {
                if (ipSBit == 1) {
                    if (leftNode.rightNode == null) {
                        leftNode.rightNode = new IpNode();
                    }
                    leftNode = leftNode.rightNode;
                } else {
                    if (leftNode.leftNode == null) {
                        leftNode.leftNode = new IpNode();
                    }
                    if (leftNode.rightNode == null) {
                        leftNode.rightNode = new IpNode();
                    }
                    leftNode.rightNode.addressCode = addressCode;
                    leftNode = leftNode.leftNode;
                }
                if (i == 31) {
                    leftNode.addressCode = addressCode;
                }

                if (ipEBit == 1) {
                    if (rightNode.rightNode == null) {
                        rightNode.rightNode = new IpNode();
                    }
                    if (rightNode.leftNode == null) {
                        rightNode.leftNode = new IpNode();
                    }
                    rightNode.leftNode.addressCode = addressCode;
                    rightNode = rightNode.rightNode;
                } else {
                    if (rightNode.leftNode == null) {
                        rightNode.leftNode = new IpNode();
                    }
                    rightNode = rightNode.leftNode;
                }
                if (i == 31) {
                    rightNode.addressCode = addressCode;
                }
            }

            ipS = ipS << 1;
            ipE = ipE << 1;
        }
    }

    public String findIp(String ip) {

        IpNode curNode = rootNode;

        int ipInt = ipToInt(ip);

        if (ipInt == -1) {
            return NO_ADDRESS;
        }

        for (int i = 0; i < 32; i++) {

            int ipSBit = (0x80000000 & ipInt) >>> 31;

            if (ipSBit == 0) {
                curNode = curNode.leftNode;
            } else {
                curNode = curNode.rightNode;
            }

            if (curNode == null) {
                return NO_ADDRESS;
            }

            if (curNode.addressCode != null && !"".equals(curNode.addressCode.trim())) {
                return curNode.addressCode;
            }

            ipInt = ipInt << 1;
        }

        return NO_ADDRESS;
    }

    private int ipToInt(String strIP) {
        try {

            int[] ip = new int[4];

            int position1 = strIP.indexOf(".");
            int position2 = strIP.indexOf(".", position1 + 1);
            int position3 = strIP.indexOf(".", position2 + 1);

            ip[0] = Integer.parseInt(strIP.substring(0, position1));
            ip[1] = Integer.parseInt(strIP.substring(position1 + 1, position2));
            ip[2] = Integer.parseInt(strIP.substring(position2 + 1, position3));
            ip[3] = Integer.parseInt(strIP.substring(position3 + 1));
            int ipInt = (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];

            return ipInt;

        } catch (Exception e) {
            return -1;
        }
    }

    private void loopTree(IpNode ipNode, int depth) {
        System.out.println(depth + "\t" + ipNode.addressCode);
        if (ipNode.leftNode != null) {
            System.out.println("left");
            loopTree(ipNode.leftNode, depth + 1);
        }
        if (ipNode.rightNode != null) {
            System.out.println("right");
            loopTree(ipNode.rightNode, depth + 1);
        }
    }

    /**
     * 二叉树节点类
     */
    private class IpNode {
        private IpNode leftNode;

        private IpNode rightNode;

        private String addressCode;

    }
}
