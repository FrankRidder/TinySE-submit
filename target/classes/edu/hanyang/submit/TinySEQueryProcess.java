package edu.hanyang.submit;

import java.io.IOException;

import edu.hanyang.indexer.DocumentCursor;
import edu.hanyang.indexer.PositionCursor;
import edu.hanyang.indexer.IntermediateList;
import edu.hanyang.indexer.IntermediatePositionalList;
import edu.hanyang.indexer.QueryPlanTree;
import edu.hanyang.indexer.QueryPlanTree.NODE_TYPE;
import edu.hanyang.indexer.QueryPlanTree.QueryPlanNode;
import edu.hanyang.indexer.QueryProcess;
import edu.hanyang.indexer.StatAPI;

import java.util.ArrayList;
import java.util.List;

public class TinySEQueryProcess implements QueryProcess {


    public void op_and_w_pos(DocumentCursor doc1, DocumentCursor doc2, int shift, IntermediatePositionalList output)
            throws IOException {
        int document1, document2;

        PositionCursor positionCursor1, positionCursor2;

        int position1, position2;


        while (!doc1.is_eol() && !doc2.is_eol()) {

            document1 = doc1.get_docid();
            document2 = doc2.get_docid();

            if (document1 < document2){

                doc1.go_next();

            } else if (document1 > document2) {

                doc2.go_next();

            } else {

                positionCursor1 = doc1.get_position_cursor();
                positionCursor2 = doc2.get_position_cursor();

                while (!positionCursor1.is_eol() && !positionCursor2.is_eol()) {

                    position1 = positionCursor1.get_pos();
                    position2 = positionCursor2.get_pos();

                    if (position1 + shift < position2) {

                        positionCursor1.go_next();
                    } else if (position1 + shift > position2) {

                        positionCursor2.go_next();
                    } else {

                        output.put_docid_and_pos(document1, position1);
                        positionCursor1.go_next();
                        positionCursor2.go_next();
                    }
                }
                doc1.go_next();
                doc2.go_next();
            }
        }
    }


    public void op_and_wo_pos(DocumentCursor doc1, DocumentCursor doc2, IntermediateList out) throws IOException {

        int document1, document2;

        while (!doc1.is_eol() && !doc2.is_eol()) {

            document1 = doc1.get_docid();
            document2 = doc2.get_docid();

            if (document1 < document2) {

                doc1.go_next();
            } else if (document1 > document2) {

                doc2.go_next();
            } else {

                out.put_docid(document1);
                doc1.go_next();
                doc2.go_next();

            }
        }
    }


    public QueryPlanTree parse_query(String query, StatAPI API) {

        QueryPlanTree tree = new QueryPlanTree();
        QueryPlanNode op_rand;
        List<QueryPlanNode> queryPlanNodes = new ArrayList<>();
        QueryPlanNode planNode;

        int shift = 0;
        boolean in_phase = false;

        String[] data = query.split(" ");

        for (String str : data) {
            if (str.charAt(0) == '"') {
                in_phase = true;
                System.out.println("phase on");
                shift = 0;
            }

            op_rand = tree.new QueryPlanNode();
            op_rand.type = NODE_TYPE.OPRAND;
            op_rand.termid = Integer.parseInt((str.replace('"', ' ')).trim());

            if (!in_phase) {

                planNode = tree.new QueryPlanNode();
                planNode.type = NODE_TYPE.OP_REMOVE_POS;
                planNode.left = op_rand;
                op_rand = planNode;

                if (queryPlanNodes.isEmpty()) {

                    queryPlanNodes.add(op_rand);

                } else {

                    planNode = tree.new QueryPlanNode();
                    planNode.type = NODE_TYPE.OP_AND;
                    planNode.left = queryPlanNodes.get(queryPlanNodes.size() - 1);
                    queryPlanNodes.remove(queryPlanNodes.size() - 1);
                    planNode.right = op_rand;
                    queryPlanNodes.add(planNode);

                }
            } else {

                if (!queryPlanNodes.isEmpty() && (queryPlanNodes.get(queryPlanNodes.size() - 1).type == NODE_TYPE.OPRAND || queryPlanNodes.get(queryPlanNodes.size() - 1).type == NODE_TYPE.OP_SHIFTED_AND)) {

                    planNode = tree.new QueryPlanNode();
                    planNode.type = NODE_TYPE.OP_SHIFTED_AND;
                    planNode.shift = shift;
                    planNode.left = queryPlanNodes.get(queryPlanNodes.size() - 1);
                    queryPlanNodes.remove(queryPlanNodes.size() - 1);
                    planNode.right = op_rand;
                    queryPlanNodes.add(planNode);

                } else {

                    queryPlanNodes.add(op_rand);
                }
                shift++;
            }

            if (str.charAt(str.length() - 1) == '"') {

                in_phase = false;
                System.out.println("phase off");
                op_rand = tree.new QueryPlanNode();
                op_rand.type = NODE_TYPE.OP_REMOVE_POS;
                op_rand.left = queryPlanNodes.get(queryPlanNodes.size() - 1);
                queryPlanNodes.remove(queryPlanNodes.size() - 1);

                if (queryPlanNodes.isEmpty()) {

                    queryPlanNodes.add(op_rand);

                } else {

                    planNode = tree.new QueryPlanNode();
                    planNode.type = NODE_TYPE.OP_AND;
                    planNode.left = queryPlanNodes.get(queryPlanNodes.size() - 1);
                    queryPlanNodes.remove(queryPlanNodes.size() - 1);
                    planNode.right = op_rand;
                    queryPlanNodes.add(planNode);

                }
            }
        }
        tree.root = queryPlanNodes.get(0);

        return tree;
    }


}
