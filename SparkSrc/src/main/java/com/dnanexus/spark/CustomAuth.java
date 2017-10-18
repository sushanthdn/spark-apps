package com.dnanexus.spark;

import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import javax.security.sasl.AuthenticationException;
import org.apache.hadoop.hive.shims.Utils;
  import org.apache.hadoop.security.UserGroupInformation;


public class CustomAuth implements PasswdAuthenticationProvider {

  public void Authenticate(String user, String password) throws AuthenticationException {
    // allow all users for now
    try {
      System.out.println("***CustomAuth  Before setting proxy UGI " + Utils.getUGI().toString());
      System.out.println("*** CustomAuth USER is user " + user);
      UserGroupInformation.createProxyUser(user, UserGroupInformation.getCurrentUser());

      System.out.println("***CustomAuth  USERname from Utils is  " + Utils.getUGI().getUserName());
      System.out.println("***CustomAuth  UGI " + Utils.getUGI().toString());
      Thread.currentThread().dumpStack();
    } catch (Exception x) {
      System.out.println("**** Exception in CustomAuth..");
      x.printStackTrace();
    }
  }


}
