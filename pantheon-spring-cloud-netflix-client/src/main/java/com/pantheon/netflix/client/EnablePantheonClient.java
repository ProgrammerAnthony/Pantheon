package com.pantheon.netflix.client;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Anthony
 * @create 2021/12/14
 * @desc All it does is turn on discovery and let the autoconfiguration
 * find the pantheon classes if they are available
 **/
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public  @interface EnablePantheonClient {
}
