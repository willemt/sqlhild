"""Relational Algebra optimizers.

This module uses pattern matching to optimize relational algebra.
"""

import logging

from .relational_algebra import (
    And,
    BoolFalse,
    BoolTrue,
    Column,
    Cross,
    EmptySet,
    Equal,
    GreaterThan,
    In,
    Intersection,
    Join,
    LessThan,
    List,
    Not,
    Number,
    Or,
    Select,
    Table,
    Theta,
    Union,
    UniverseSet,
    pretty_print,
)

from matchpy import (
    Arity,
    CustomConstraint,
    Operation,
    Pattern,
    ReplacementRule,
    Symbol,
    Wildcard,
    replace_all,
)


logger = logging.getLogger(__name__)


# a_true = Wildcard.dot('a_true', BoolTrue)
# a_false = Wildcard.dot('a_false', BoolFalse)
a_num = Wildcard.symbol('a_num', Number)
b_num = Wildcard.symbol('b_num', Number)
a = Wildcard.dot('a')
b = Wildcard.dot('b')
c = Wildcard.dot('c')
d = Wildcard.dot('d')
e = Wildcard.dot('e')
f = Wildcard.dot('f')
star = Wildcard.star('star')
star_a = Wildcard.star('star_a')
star_b = Wildcard.star('star_b')
plus = Wildcard.plus('plus')
plus_a = Wildcard.plus('plus_a')


θ = Theta


def equijoin(a, c, b, d):
    return Equal(Column(a, c), Column(b, d))


cross_with_universe = ReplacementRule(
    Pattern(Cross(UniverseSet(), plus_a)),
    lambda plus_a: Cross(*plus_a)
)

remove_empty_select = ReplacementRule(
    Pattern(Select(a, And())),
    lambda a: a
)

convert_union_into_select = ReplacementRule(
    Pattern(Union(Select(a, b), Select(a, c))),
    lambda a, b, c: Select(a, Or(b, c))
)


"""
σ θ(R × S) = R ⋈ θ S
"""
cross_plus_select_2_join = ReplacementRule(
    Pattern(
        Select(
            Cross(a, b, star_a),
            And(equijoin(a, c, b, d), star))),
    lambda a, b, c, d, star, star_a:
        Select(
            Cross(
                Join(θ(a, Column(a, c)), θ(b, Column(b, d))),
                *star_a),
            And(*star))
)

join2 = ReplacementRule(
    Pattern(
        Select(
            Cross(a, Join(θ(b, Column(b, d)), star_a)),
            And(equijoin(a, c, b, e), star))),
    lambda a, b, c, d, e, star, star_a:
        Select(
            Join(
                θ(a, Column(a, c)),
                θ(
                    Join(θ(b, Column(b, d)), *star_a),
                    Column(b, e))),
            And(*star))
)

cross_reduction = ReplacementRule(
    Pattern(Cross(a, a, star_a)),
    lambda a, star_a: Cross(a, *star_a)
)


def aaa(a, b, c, d, e, f):
    # TODO: check that A contains table C with column D
    # TODO: check that B contains table E with column F
    print('a', a)
    print('b', b)
    print('c', c)
    print('d', d)
    print('e', e)
    print('f', f)
    return True


join3 = ReplacementRule(
    Pattern(
        Select(
            Cross(a, b),
            Equal(Column(c, d), Column(e, f))),
        CustomConstraint(aaa)
        ),
    lambda a, b, c, d, e, f:
        Join(θ(a, Column(c, d)), θ(b, Column(e, f)))
)

"""
σA(B)⋂B = σA(B)
"""

intersection_select_of_same_relation = ReplacementRule(
    Pattern(Intersection(a, Select(a, b), star_a)),
    lambda a, b, star_a: Intersection(Select(a, b), *star_a)
)

intersection_of_same_relation = ReplacementRule(
    Pattern(Intersection(a, a, star_a)),
    lambda a, star_a: Intersection(a, *star_a)
)

union_of_same_relation = ReplacementRule(
    Pattern(Union(a, Select(a, b), star_a)),
    lambda a, b, star_a: Union(a, *star_a)
)

"""
σA(B)⋃σA(B) = σA(B)
"""
union_of_equilvalent_selects = ReplacementRule(
    Pattern(Union(Select(a, b), Select(a, b), star_a)),
    lambda a, b, star_a: Union(Select(a, b), *star_a)
)

"""
σA(B)⋂σA(B) = σA(B)
"""
intersection_of_equilvalent_selects = ReplacementRule(
    Pattern(Intersection(Select(a, b), Select(a, b), star_a)),
    lambda a, b, star_a: Intersection(Select(a, b), *star_a)
)

"""
A⋃(σB(A)⋂σC(A)) = A
"""

union_of_same_relation_over_intersection = ReplacementRule(
    Pattern(Union(a, Intersection(Select(a, b), star_b), star_a)),
    lambda a, b, star_a, star_b: Union(a, Intersection(*star_b), *star_a)
)


# shift_joins = ReplacementRule(
#     Pattern(ThetaJoin(a, ThetaJoin(b, star_b), c)),
#     lambda a, b, c, star_b: ThetaJoin(b, ThetaJoin(a, *star_b), c)
# )


"""
Convenience
"""

swap_comparison = ReplacementRule(
    Pattern(GreaterThan(Column(a, c), Column(b, d))),
    lambda a, b, c, d: LessThan(Column(b, d), Column(a, c))
)

"""
σA(R)⋂B(R) = σA⋀B(R)
Combining selects is good if it means we seek over the same relation less.
"""
combine_selects = ReplacementRule(
    Pattern(Intersection(Select(Cross(plus), And(a)), Select(Cross(plus), b), star)),
    lambda a, b, plus, star: Intersection(Select(Cross(*plus), And(a, b)), *star)
)

# merge_less_thans = ReplacementRule(
#     Pattern(And(LessThan(Column(a, b), Number(c)), LessThan(Column(a, b), Number(d)))),
#     # lambda a, b, c, d, star: And(LessThan(Column(a, b), Number(c) if c < d else Number(d)), *star)
#     lambda a, b, c, d: And(LessThan(Column(a, b), Number(c)))
# )

merge_less_thans = ReplacementRule(
    Pattern(And(LessThan(a, a_num), LessThan(a, b_num), star)),
    lambda a, a_num, b_num, star: And(LessThan(a, a_num if a_num < b_num else b_num), *star)
)

"""
 x.y > a ⋀ x.y > b = x.y > a
Combine comparators if one is superseded
This is useful for removing redundant filters.
"""
merge_greater_thans = ReplacementRule(
    Pattern(And(GreaterThan(a, a_num), GreaterThan(a, b_num), star)),
    lambda a, a_num, b_num, star: And(GreaterThan(a, a_num if a_num > b_num else b_num), *star)
)

"""
"""

"""
Expression that is a tautology
"""

tautology = ReplacementRule(
    Pattern(Equal(a, a)),
    lambda a: BoolTrue()
)

and_tautology = ReplacementRule(
    Pattern(And(BoolTrue(), plus_a)),
    lambda plus_a: And(*plus_a)
)


"""
a ⋂ True = a
"""
intersect_true = ReplacementRule(
    Pattern(Intersection(BoolTrue(), a, star_a)),
    lambda a, star_a: Intersection(a, *star_a)
)

"""
a ⋀ False = False
"""
logic_and_false = ReplacementRule(
    Pattern(And(BoolFalse(), plus_a)),
    lambda plus_a: And(BoolFalse())
)

"""
a ⋀ True = a
"""
logic_and_true = ReplacementRule(
    Pattern(And(BoolTrue(), plus_a)),
    lambda plus_a: And(*plus_a)
)


"""
a ∨ False = a
"""
logic_or_false = ReplacementRule(
    Pattern(Or(BoolFalse(), plus_a)),
    lambda plus_a: And(*plus_a)
)

"""
a ∨ True = True
"""
logic_or_true = ReplacementRule(
    Pattern(Or(BoolTrue(), plus_a)),
    lambda plus_a: And(BoolTrue())
)

select_tautology = ReplacementRule(
    Pattern(Select(a, BoolTrue())),
    lambda a: a
)

equal_column_tautology = ReplacementRule(
    Pattern(Equal(Column(a, b), Column(a, b))),
    lambda a, b: BoolTrue()
)

# or_tautology = ReplacementRule(
#     Pattern(Or(a, plus_a)),
#     lambda a, plus_a: Or(a)
# )

# push_down = ReplacementRule(
#     Pattern(Select(Equal(Column(c, d), Column(e, f)), Cross(a, b))),
#     lambda a, b, c, d, e, f: ThetaJoin(Equal(Column(c, d), Column(e, f)), a, b)
# )


"""
σ((OneToTen X TwoToTwentyInTwos), (OneToTen.val == 0))) = σ(OneToTen, (OneToTen.val == 0)) X TwoToTwentyInTwos
It's good to push selects as far down as possible to reduce cross costs.
"""
push_select_down_cross = ReplacementRule(
    Pattern(Select(Cross(a, e), Equal(Column(a, b), c))),
    lambda a, b, c, e: Cross(Select(a, Equal(Column(a, b), c)), e)
)


"""
Convert X in (1 .. 9) to X = 1 or ... or X = 9
"""
in_statement_2_ors = ReplacementRule(
    Pattern(In(a, List(b, star_a))),
    lambda a, b, star_a: Or(Equal(a, b), In(a, List(*star_a)))
)

"""
X in () is False
"""
in_statement_false = ReplacementRule(
    Pattern(In(a, List())),
    lambda a: BoolFalse()
)


"""
a ⋀ a = a
"""
logic_idempotence_and = ReplacementRule(
    Pattern(And(a, a, star_a)),
    lambda a, plus_a: And(a, *star_a)
)


"""
a ∨ a = a
"""
logic_idempotence_or = ReplacementRule(
    Pattern(Or(a, a, star_a)),
    lambda a, plus_a: Or(a, *star_a)
)

"""
!!a = a
"""
logic_double_negation = ReplacementRule(
    Pattern(Not(Not(a))),
    lambda a: a
)


false_becomes_emptyset = ReplacementRule(
    Pattern(BoolFalse()),
    lambda: EmptySet()
)

"""
∅⋃A = A
"""
union_of_empty_set_is_ignored = ReplacementRule(
    Pattern(Union(EmptySet(), plus_a)),
    lambda plus_a: Union(*plus_a)
)

"""
∅⋂A = A
"""
intersection_of_empty_set_is_empty_set = ReplacementRule(
    Pattern(Intersection(EmptySet(), plus_a)),
    lambda plus_a: EmptySet()
)

# empty_union = ReplacementRule(
#     Pattern(Union()),
#     lambda: EmptySet()
# )


def remove_universe_set(algebra):
    rules = [
        cross_with_universe,
    ]
    return replace_all(algebra, rules)


def optimize(algebra):
    rules = [
        # cross_reduction,
        # tautology,
        # and_tautology,
        cross_with_universe,
        remove_empty_select,
        convert_union_into_select,
        logic_and_true,
        logic_or_true,
        logic_or_false,
        intersect_true,
        select_tautology,
        equal_column_tautology,
        intersection_select_of_same_relation,
        intersection_of_same_relation,
        union_of_same_relation,
        union_of_equilvalent_selects,
        intersection_of_equilvalent_selects,
        union_of_same_relation_over_intersection,
        combine_selects,
        cross_plus_select_2_join,
        join2,
        # join3,
        swap_comparison,
        merge_greater_thans,
        merge_less_thans,
        false_becomes_emptyset,
        union_of_empty_set_is_ignored,
        intersection_of_empty_set_is_empty_set,
        push_select_down_cross,
        # empty_union,
        # shift_joins,
        in_statement_2_ors,
        in_statement_false,
    ]
    new_algebra = replace_all(algebra, rules)
    new_algebra._tables = algebra._tables

    logger.debug("Optimized RA:\n{}".format(pretty_print(new_algebra)))

    return new_algebra
