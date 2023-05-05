package CSCI485ClassProject.utils;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.ComparisonPredicate;

public class ComparisonUtils {
  public static long convertObjectToLong(Object o)
  {
    long val1;
    if (o instanceof Integer) {
      val1 = new Long((Integer) o);
    } else {
      val1 = (long) o;
    }
    return val1;
  }

  public static boolean checkComparisonPredicateTypes(ComparisonPredicate cp)
  {
    if (cp.getPredicateType() == ComparisonPredicate.Type.TWO_ATTRS)
    {
      AttributeType leftType = cp.getLeftHandSideAttrType();
      AttributeType rightType = cp.getRightHandSideAttrType();
      if (leftType != rightType)
      {
        System.out.println("Types don't match");
        return false;
      }
    }

    return true;
  }

  public static boolean compareTwoObjects(Object obj1, Object obj2, ComparisonPredicate cmp)
  {
    if (!checkComparisonPredicateTypes(cmp))
      return false;
    AttributeType objTypes = cmp.getLeftHandSideAttrType();
    if (objTypes == AttributeType.INT)
    {
      return compareTwoINT(obj1, obj2, cmp.getOperator());
    }
    else if (objTypes == AttributeType.DOUBLE)
    {
      return compareTwoDOUBLE(obj1, obj2, cmp.getOperator());
    }
    else {
      return compareTwoVARCHAR(obj1, obj2, cmp.getOperator());
    }
  }


  public static boolean compareTwoINT(Object obj1, Object obj2, ComparisonOperator cmp) {
    long val1;
    if (obj1 instanceof Integer) {
      val1 = new Long((Integer) obj1);
    } else {
      val1 = (long) obj1;
    }

    long val2;
    if (obj2 instanceof Integer) {
      val2 = new Long((Integer) obj2);
    } else {
      val2 = (long) obj2;
    }

    if (cmp == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) {
      // >=
      return val1 >= val2;

    } else if (cmp == ComparisonOperator.GREATER_THAN) {
      // >
      return val1 > val2;
    } else if (cmp == ComparisonOperator.EQUAL_TO) {
      // ==
      return val1 == val2;
    } else if (cmp == ComparisonOperator.LESS_THAN) {
      // <
      return val1 < val2;
    } else {
      // <=
      return val1 <= val2;
    }
  }

  public static boolean compareTwoDOUBLE(Object obj1, Object obj2, ComparisonOperator cmp) {
    double val1 = (double) obj1;
    double val2 = (double) obj2;

    if (cmp == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) {
      // >=
      return val1 >= val2;

    } else if (cmp == ComparisonOperator.GREATER_THAN) {
      // >
      return val1 > val2;
    } else if (cmp == ComparisonOperator.EQUAL_TO) {
      // ==
      return val1 == val2;
    } else if (cmp == ComparisonOperator.LESS_THAN) {
      // <
      return val1 < val2;
    } else {
      // <=
      return val1 <= val2;
    }
  }

  public static boolean compareTwoVARCHAR(Object obj1, Object obj2, ComparisonOperator cmp) {
    String val1 = (String) obj1;
    String val2 = (String) obj2;

    if (cmp == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) {
      // >=
      return val1.equals(val2) || val1.compareTo(val2) > 0;

    } else if (cmp == ComparisonOperator.GREATER_THAN) {
      // >
      return val1.compareTo(val2) > 0;
    } else if (cmp == ComparisonOperator.EQUAL_TO) {
      // ==
      return val1.equals(val2);
    } else if (cmp == ComparisonOperator.LESS_THAN) {
      // <
      return val1.compareTo(val2) < 0;
    } else {
      // <=
      return val1.compareTo(val2) < 0 || val1.equals(val2);
    }
  }

}
