package com.pantheon.common;


import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * @author Anthony
 * @create 2021/11/19
 * @desc
 **/
public class ObjectUtils {

    /**
     * Determine whether the given object is empty.
     * <p>This method supports the following object types.
     * <ul>
     * <li>{@code Optional}: considered empty if {@link Optional#empty()}</li>
     * <li>{@code Array}: considered empty if its length is zero</li>
     * <li>{@link CharSequence}: considered empty if its length is zero</li>
     * <li>{@link Collection}: delegates to {@link Collection#isEmpty()}</li>
     * <li>{@link Map}: delegates to {@link Map#isEmpty()}</li>
     * <li>{@link Integer}: considered empty if its num is 0</li>
     * <li>{@link Long}: considered empty if its num is 0L</li>
     * </ul>
     * <p>If the given object is non-null and not one of the aforementioned
     * supported types, this method returns {@code false}.
     *
     * @param obj the object to check
     * @return {@code true} if the object is {@code null} or <em>empty</em>
     */
    @SuppressWarnings("rawtypes")
    public static boolean isEmpty(Object obj) {
        if (obj == null) {
            return true;
        }

        if (obj instanceof Optional) {
            return !((Optional) obj).isPresent();
        }
        if (obj instanceof CharSequence) {
            return ((CharSequence) obj).length() == 0;
        }
        if (obj.getClass().isArray()) {
            return Array.getLength(obj) == 0;
        }
        if (obj instanceof Collection) {
            return ((Collection) obj).isEmpty();
        }
        if (obj instanceof Map) {
            return ((Map) obj).isEmpty();
        }
        //0 is empty
        if (obj instanceof Integer) {
            return ((Integer) obj) == 0;
        }
        if (obj instanceof Long) {
            return ((Long) obj) == 0L;
        }
        // else
        return false;
    }
}
