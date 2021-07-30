package org.pipecraft.infra.concurrent;

import java.util.Objects;

public interface FailablePredicate <T, E extends Exception> {

  /**
   * Evaluates this predicate on the given argument.
   *
   * @param t the input argument
   * @return {@code true} if the input argument matches the predicate,
   * otherwise {@code false}
   * @throws E in case of a failure evaluating the predicate
   */
  boolean test(T t) throws E;

  /**
   * Returns a composed predicate that represents a short-circuiting logical
   * AND of this predicate and another.  When evaluating the composed
   * predicate, if this predicate is {@code false}, then the {@code other}
   * predicate is not evaluated.
   *
   * <p>Any exceptions thrown during evaluation of either predicate are relayed
   * to the caller; if evaluation of this predicate throws an exception, the
   * {@code other} predicate will not be evaluated.
   *
   * @param other a predicate that will be logically-ANDed with this
   *              predicate
   * @return a composed predicate that represents the short-circuiting logical
   * AND of this predicate and the {@code other} predicate
   * @throws NullPointerException if other is null
   */
  default FailablePredicate<T, E> and(FailablePredicate<? super T, ? extends E> other) {
    Objects.requireNonNull(other);
    return (t) -> test(t) && other.test(t);
  }

  /**
   * @return a predicate that represents the logical negation of this
   * predicate
   */
  default FailablePredicate<T, E> negate() {
    return (t) -> !test(t);
  }

  /**
   * Returns a composed predicate that represents a short-circuiting logical
   * OR of this predicate and another.  When evaluating the composed
   * predicate, if this predicate is {@code true}, then the {@code other}
   * predicate is not evaluated.
   *
   * <p>Any exceptions thrown during evaluation of either predicate are relayed
   * to the caller; if evaluation of this predicate throws an exception, the
   * {@code other} predicate will not be evaluated.
   *
   * @param other a predicate that will be logically-ORed with this
   *              predicate
   * @return a composed predicate that represents the short-circuiting logical
   * OR of this predicate and the {@code other} predicate
   * @throws NullPointerException if other is null
   */
  default FailablePredicate<T, E> or(FailablePredicate<? super T, ? extends E> other) {
    Objects.requireNonNull(other);
    return (t) -> test(t) || other.test(t);
  }
}
