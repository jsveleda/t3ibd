/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table;

/**
 *
 * @author Sergio
 */
public class ComparisonTypes {

    public static final int EQUAL = 1;
    public static final int DIFF = 2;
    public static final int GREATER_THAN = 3;
    public static final int GREATER_EQUAL_THAN = 4;
    public static final int LOWER_THAN = 5;
    public static final int LOWER_EQUAL_THAN = 6;

    public static String getComparisonType(int type) {
        switch (type) {
            case EQUAL:
                return "EQUAL";
            case DIFF:
                return "DIFF";
            case GREATER_THAN:
                return "GREATER_THAN";
            case GREATER_EQUAL_THAN:
                return "GREATER_EQUAL_THAN";
            case LOWER_THAN:
                return "LOWER_THAN";
            case LOWER_EQUAL_THAN:
                return "LOWER_EQUAL_THAN";
            default:
                return "---";

        }
    }

}
