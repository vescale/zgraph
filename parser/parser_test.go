package parser_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vescale/zgraph/parser"
	"github.com/vescale/zgraph/parser/format"
)

type testCase struct {
	src     string
	ok      bool
	restore string
}

func TestQuery(t *testing.T) {
	table := []testCase{
		{"SELECT label(n) AS lbl, COUNT(*) FROM MATCH (n) GROUP BY lbl ORDER BY COUNT(*) DESC", true, "SELECT LABEL(`n`) AS `lbl`,COUNT(1) FROM MATCH (`n`) GROUP BY `lbl` ORDER BY COUNT(1) DESC"},
		{`SELECT label(n) AS srcLbl, label(e) AS edgeLbl, label(m) AS dstLbl, COUNT(*)
		   FROM MATCH (n) -[e]-> (m)
		GROUP BY srcLbl, edgeLbl, dstLbl
		ORDER BY COUNT(*) DESC`, true, "SELECT LABEL(`n`) AS `srcLbl`,LABEL(`e`) AS `edgeLbl`,LABEL(`m`) AS `dstLbl`,COUNT(1) FROM MATCH (`n`) -[`e`]-> (`m`) GROUP BY `srcLbl`,`edgeLbl`,`dstLbl` ORDER BY COUNT(1) DESC"},
		{"SELECT n.name, n.dob FROM MATCH (n:Person)", true, "SELECT `n`.`name`,`n`.`dob` FROM MATCH (`n`:`Person`)"},
		{"SELECT a.name AS a, b.name AS b FROM MATCH (a:Person) -[e:knows]-> (b:Person)", true, "SELECT `a`.`name` AS `a`,`b`.`name` AS `b` FROM MATCH (`a`:`Person`) -[`e`:`knows`]-> (`b`:`Person`)"},
		{"SELECT n.name, n.dob FROM MATCH (n:Person|University)", true, "SELECT `n`.`name`,`n`.`dob` FROM MATCH (`n`:`Person`|`University`)"},
		{"SELECT n.name, n.dob FROM MATCH (n)", true, "SELECT `n`.`name`,`n`.`dob` FROM MATCH (`n`)"},
		{"SELECT n.name, n.dob FROM MATCH (n) WHERE n.dob > DATE '1995-01-01'", true, "SELECT `n`.`name`,`n`.`dob` FROM MATCH (`n`) WHERE `n`.`dob`>DATE '1995-01-01'"},
		{"SELECT m.name AS name, m.dob AS dob FROM MATCH (n) -[e]-> (m) WHERE n.name = 'Kathrine' AND n.dob <= m.dob", true, "SELECT `m`.`name` AS `name`,`m`.`dob` AS `dob` FROM MATCH (`n`) -[`e`]-> (`m`) WHERE `n`.`name`='Kathrine' AND `n`.`dob`<=`m`.`dob`"},
		{"SELECT p2.name AS friend, u.name AS university FROM MATCH (u:University) <-[:studentOf]- (p1:Person) -[:knows]-> (p2:Person) -[:studentOf]-> (u) WHERE p1.name = 'Lee'", true, "SELECT `p2`.`name` AS `friend`,`u`.`name` AS `university` FROM MATCH (`u`:`University`) <-[:`studentOf`]- (`p1`:`Person`) -[:`knows`]-> (`p2`:`Person`) -[:`studentOf`]-> (`u`) WHERE `p1`.`name`='Lee'"},
		{`SELECT p2.name AS friend, u.name AS university
  FROM MATCH (p1:Person) -[:knows]-> (p2:Person)
     , MATCH (p1) -[:studentOf]-> (u:University)
     , MATCH (p2) -[:studentOf]-> (u)
 WHERE p1.name = 'Lee'`, true, "SELECT `p2`.`name` AS `friend`,`u`.`name` AS `university` FROM MATCH (`p1`:`Person`) -[:`knows`]-> (`p2`:`Person`),MATCH (`p1`) -[:`studentOf`]-> (`u`:`University`),MATCH (`p2`) -[:`studentOf`]-> (`u`) WHERE `p1`.`name`='Lee'"},
		{`SELECT p1.name AS p1, p2.name AS p2, p3.name AS p3
  FROM MATCH (p1:Person) -[:knows]-> (p2:Person) -[:knows]-> (p3:Person)
 WHERE p1.name = 'Lee'`, true, "SELECT `p1`.`name` AS `p1`,`p2`.`name` AS `p2`,`p3`.`name` AS `p3` FROM MATCH (`p1`:`Person`) -[:`knows`]-> (`p2`:`Person`) -[:`knows`]-> (`p3`:`Person`) WHERE `p1`.`name`='Lee'"},
		{`SELECT p1.name AS p1, p2.name AS p2, p3.name AS p3
  FROM MATCH (p1:Person) -[:knows]-> (p2:Person) -[:knows]-> (p3:Person)
 WHERE p1.name = 'Lee' AND p1 <> p3`, true, "SELECT `p1`.`name` AS `p1`,`p2`.`name` AS `p2`,`p3`.`name` AS `p3` FROM MATCH (`p1`:`Person`) -[:`knows`]-> (`p2`:`Person`) -[:`knows`]-> (`p3`:`Person`) WHERE `p1`.`name`='Lee' AND `p1`<>`p3`"},
		{`SELECT p1.name AS p1, p2.name AS p2, p3.name AS p3
  FROM MATCH (p1:Person) -[:knows]-> (p2:Person) -[:knows]-> (p3:Person)
 WHERE p1.name = 'Lee' AND ALL_DIFFERENT(p1, p3)`, true, "SELECT `p1`.`name` AS `p1`,`p2`.`name` AS `p2`,`p3`.`name` AS `p3` FROM MATCH (`p1`:`Person`) -[:`knows`]-> (`p2`:`Person`) -[:`knows`]-> (`p3`:`Person`) WHERE `p1`.`name`='Lee' AND ALL_DIFFERENT(`p1`, `p3`)"},
		{`SELECT p1.name AS p1, p2.name AS p2, e1 = e2
  FROM MATCH (p1:Person) -[e1:knows]-> (riya:Person)
     , MATCH (p2:Person) -[e2:knows]-> (riya)
 WHERE riya.name = 'Riya'`, true, "SELECT `p1`.`name` AS `p1`,`p2`.`name` AS `p2`,`e1`=`e2` FROM MATCH (`p1`:`Person`) -[`e1`:`knows`]-> (`riya`:`Person`),MATCH (`p2`:`Person`) -[`e2`:`knows`]-> (`riya`) WHERE `riya`.`name`='Riya'"},
		{"SELECT * FROM MATCH (n) -[e1]- (m) -[e2]- (o)", true, "SELECT * FROM MATCH (`n`) -[`e1`]- (`m`) -[`e2`]- (`o`)"},
		{"SELECT n, m, n.age AS age FROM MATCH (n:Person) -[e:friend_of]-> (m:Person)", true, "SELECT `n`,`m`,`n`.`age` AS `age` FROM MATCH (`n`:`Person`) -[`e`:`friend_of`]-> (`m`:`Person`)"},
		{"SELECT n.age * 2 - 1 AS pivot, n.name, n FROM MATCH (n:Person) -> (m:Car) ORDER BY pivot", true, "SELECT `n`.`age`*2-1 AS `pivot`,`n`.`name`,`n` FROM MATCH (`n`:`Person`) -> (`m`:`Car`) ORDER BY `pivot`"},
		{"SELECT * FROM MATCH (n:Person) -> (m) -> (w), MATCH (n) -> (w) -> (m)", true, "SELECT * FROM MATCH (`n`:`Person`) -> (`m`) -> (`w`),MATCH (`n`) -> (`w`) -> (`m`)"},
		{"SELECT n, m, w FROM MATCH (n:Person) -> (m) -> (w), MATCH (n) -> (w) -> (m)", true, "SELECT `n`,`m`,`w` FROM MATCH (`n`:`Person`) -> (`m`) -> (`w`),MATCH (`n`) -> (`w`) -> (`m`)"},
		{`SELECT p.first_name, p.last_name
    FROM MATCH (p:Person) ON my_graph
ORDER BY p.first_name, p.last_name`, true, "SELECT `p`.`first_name`,`p`.`last_name` FROM MATCH (`p`:`Person`) ON `my_graph` ORDER BY `p`.`first_name`,`p`.`last_name`"},
		{"SELECT p.first_name, p.last_name FROM MATCH (p:Person) ORDER BY p.first_name, p.last_name", true, "SELECT `p`.`first_name`,`p`.`last_name` FROM MATCH (`p`:`Person`) ORDER BY `p`.`first_name`,`p`.`last_name`"},
		{"SELECT * FROM MATCH (n) -[e1]-> (m1), MATCH (n) -[e2]-> (m2)", true, "SELECT * FROM MATCH (`n`) -[`e1`]-> (`m1`),MATCH (`n`) -[`e2`]-> (`m2`)"},
		{"SELECT * FROM MATCH (n1) -[e1]-> (n2) -[e2]-> (n3) -[e3]-> (n4)", true, "SELECT * FROM MATCH (`n1`) -[`e1`]-> (`n2`) -[`e2`]-> (`n3`) -[`e3`]-> (`n4`)"},
		{"SELECT * FROM MATCH (n1) -[e1]-> (n2), MATCH (n2) -[e2]-> (n3), MATCH (n3) -[e3]-> (n4)", true, "SELECT * FROM MATCH (`n1`) -[`e1`]-> (`n2`),MATCH (`n2`) -[`e2`]-> (`n3`),MATCH (`n3`) -[`e3`]-> (`n4`)"},
		{"SELECT * FROM MATCH (n1) -[e1]-> (n2) <-[e2]- (n3)", true, "SELECT * FROM MATCH (`n1`) -[`e1`]-> (`n2`) <-[`e2`]- (`n3`)"},
		{"SELECT * FROM MATCH (n1) -> (m1), MATCH (n2) -> (m2)", true, "SELECT * FROM MATCH (`n1`) -> (`m1`),MATCH (`n2`) -> (`m2`)"},
		{"SELECT * FROM MATCH (x:Person) -[e:likes|knows]-> (y:Person)", true, "SELECT * FROM MATCH (`x`:`Person`) -[`e`:`likes`|`knows`]-> (`y`:`Person`)"},
		{"SELECT * FROM MATCH (:Person) -[:likes|knows]-> (:Person)", true, "SELECT * FROM MATCH (:`Person`) -[:`likes`|`knows`]-> (:`Person`)"},
		{"SELECT y.name FROM MATCH (x) -> (y) WHERE x.name = 'Jake' AND y.age > 25", true, "SELECT `y`.`name` FROM MATCH (`x`) -> (`y`) WHERE `x`.`name`='Jake' AND `y`.`age`>25"},
		{"SELECT y.name FROM MATCH (x) -> (y) WHERE y.age > 25 AND x.name = 'Jake'", true, "SELECT `y`.`name` FROM MATCH (`x`) -> (`y`) WHERE `y`.`age`>25 AND `x`.`name`='Jake'"},
		{"SELECT n.first_name, COUNT(*), AVG(n.age) FROM MATCH (n:Person) GROUP BY n.first_name", true, "SELECT `n`.`first_name`,COUNT(1),AVG(`n`.`age`) FROM MATCH (`n`:`Person`) GROUP BY `n`.`first_name`"},
		{"SELECT n.first_name, n.last_name, COUNT(*) FROM MATCH (n:Person) GROUP BY n.first_name, n.last_name", true, "SELECT `n`.`first_name`,`n`.`last_name`,COUNT(1) FROM MATCH (`n`:`Person`) GROUP BY `n`.`first_name`,`n`.`last_name`"},
		{`SELECT n.prop1, n.prop2, COUNT(*)
    FROM MATCH (n)
GROUP BY n.prop1, n.prop2
  HAVING n.prop1 IS NOT NULL AND n.prop2 IS NOT NULL`, true, "SELECT `n`.`prop1`,`n`.`prop2`,COUNT(1) FROM MATCH (`n`) GROUP BY `n`.`prop1`,`n`.`prop2` HAVING `n`.`prop1` IS NOT NULL AND `n`.`prop2` IS NOT NULL"},
		{" SELECT n.age, COUNT(*) FROM MATCH (n) GROUP BY n.age ORDER BY n.age", true, "SELECT `n`.`age`,COUNT(1) FROM MATCH (`n`) GROUP BY `n`.`age` ORDER BY `n`.`age`"},
		{`SELECT label(owner),
		      COUNT(*) AS numTransactions,
		      SUM(out.amount) AS totalOutgoing,
		      LISTAGG(out.amount, ', ') AS amounts
		 FROM MATCH (a:Account) -[:owner]-> (owner:Person|Company)
		    , MATCH (a) -[out:transaction]-> (:Account)
		GROUP BY label(owner)
		ORDER BY label(owner)`, true, "SELECT LABEL(`owner`),COUNT(1) AS `numTransactions`,SUM(`out`.`amount`) AS `totalOutgoing`,LISTAGG(`out`.`amount`, ', ') AS `amounts` FROM MATCH (`a`:`Account`) -[:`owner`]-> (`owner`:`Person`|`Company`),MATCH (`a`) -[`out`:`transaction`]-> (:`Account`) GROUP BY LABEL(`owner`) ORDER BY LABEL(`owner`)"},
		{`SELECT COUNT(*) AS numTransactions,
       SUM(out.amount) AS totalOutgoing,
       LISTAGG(out.amount, ', ') AS amounts
  FROM MATCH (a:Account) -[:owner]-> (owner:Person|Company)
     , MATCH (a) -[out:transaction]-> (:Account)`, true, "SELECT COUNT(1) AS `numTransactions`,SUM(`out`.`amount`) AS `totalOutgoing`,LISTAGG(`out`.`amount`, ', ') AS `amounts` FROM MATCH (`a`:`Account`) -[:`owner`]-> (`owner`:`Person`|`Company`),MATCH (`a`) -[`out`:`transaction`]-> (:`Account`)"},
		{"SELECT COUNT(*) FROM MATCH (m:Person)", true, "SELECT COUNT(1) FROM MATCH (`m`:`Person`)"},
		{"SELECT AVG(DISTINCT m.age) FROM MATCH (m:Person)", true, "SELECT AVG(DISTINCT `m`.`age`) FROM MATCH (`m`:`Person`)"},
		{"SELECT n.name FROM MATCH (n) -[:has_friend]-> (m) GROUP BY n HAVING COUNT(m) > 10", true, "SELECT `n`.`name` FROM MATCH (`n`) -[:`has_friend`]-> (`m`) GROUP BY `n` HAVING COUNT(`m`)>10"},
		{"SELECT n.name FROM MATCH (n:Person) ORDER BY n.age ASC", true, "SELECT `n`.`name` FROM MATCH (`n`:`Person`) ORDER BY `n`.`age` ASC"},
		{"SELECT f.name FROM MATCH (f:Person) ORDER BY f.age ASC, f.salary DESC", true, "SELECT `f`.`name` FROM MATCH (`f`:`Person`) ORDER BY `f`.`age` ASC,`f`.`salary` DESC"},
		{"SELECT n FROM MATCH (n) LIMIT 10 OFFSET 5", true, "SELECT `n` FROM MATCH (`n`) LIMIT 5,10"},
		{`SELECT a.number AS a,
       b.number AS b,
       COUNT(e) AS pathLength,
       ARRAY_AGG(e.amount) AS amounts
  FROM MATCH ANY SHORTEST (a:Account) -[e:transaction]->* (b:Account)
 WHERE a.number = 10039 AND b.number = 2090`, true, "SELECT `a`.`number` AS `a`,`b`.`number` AS `b`,COUNT(`e`) AS `pathLength`,ARRAY_AGG(`e`.`amount`) AS `amounts` FROM MATCH ANY SHORTEST (`a`:`Account`) -[`e`:`transaction`]->* (`b`:`Account`) WHERE `a`.`number`=10039 AND `b`.`number`=2090"},
		{`SELECT dst.number
    FROM MATCH ANY (src:Account) -[e]->+ (dst:Account)
   WHERE src.number = 8021
ORDER BY dst.number`, true, "SELECT `dst`.`number` FROM MATCH ANY (`src`:`Account`) -[`e`]->+ (`dst`:`Account`) WHERE `src`.`number`=8021 ORDER BY `dst`.`number`"},
		{`SELECT dst.number, LISTAGG(e.amount, ' + ') || ' = ', SUM(e.amount)
    FROM MATCH ANY (src:Account) -[e]->+ (dst:Account)
   WHERE src.number = 8021
ORDER BY dst.number`, true, "SELECT `dst`.`number`,LISTAGG(`e`.`amount`, ' + ')||' = ',SUM(`e`.`amount`) FROM MATCH ANY (`src`:`Account`) -[`e`]->+ (`dst`:`Account`) WHERE `src`.`number`=8021 ORDER BY `dst`.`number`"},
		{`SELECT c.name
  FROM MATCH (c:Class) -/:subclass_of*/-> (arrayList:Class)
 WHERE arrayList.name = 'ArrayList'`, true, "SELECT `c`.`name` FROM MATCH (`c`:`Class`) -/:`subclass_of`*/-> (`arrayList`:`Class`) WHERE `arrayList`.`name`='ArrayList'"},
		{`SELECT y.name
  FROM MATCH (x:Person) -/:likes*/-> (y)
 WHERE x.name = 'Amy'`, true, "SELECT `y`.`name` FROM MATCH (`x`:`Person`) -/:`likes`*/-> (`y`) WHERE `x`.`name`='Amy'"},
		{`SELECT y.name
  FROM MATCH (x:Person) -/:likes+/-> (y)
 WHERE x.name = 'Amy'`, true, "SELECT `y`.`name` FROM MATCH (`x`:`Person`) -/:`likes`+/-> (`y`) WHERE `x`.`name`='Amy'"},
		{`SELECT y.name
  FROM MATCH (x:Person) -/:knows+/-> (y)
 WHERE x.name = 'Judith'`, true, "SELECT `y`.`name` FROM MATCH (`x`:`Person`) -/:`knows`+/-> (`y`) WHERE `x`.`name`='Judith'"},
		{`SELECT y.name
  FROM MATCH (x:Person) -/:knows?/-> (y)
 WHERE x.name = 'Judith'`, true, "SELECT `y`.`name` FROM MATCH (`x`:`Person`) -/:`knows`?/-> (`y`) WHERE `x`.`name`='Judith'"},
		{`SELECT y.name
  FROM MATCH (x:Person) -/:likes{2}/-> (y)
 WHERE x.name = 'Amy'`, true, "SELECT `y`.`name` FROM MATCH (`x`:`Person`) -/:`likes`{2}/-> (`y`) WHERE `x`.`name`='Amy'"},
		{`SELECT y.name
  FROM MATCH (x:Person) -/:likes{2,}/-> (y)
 WHERE x.name = 'Amy'`, true, "SELECT `y`.`name` FROM MATCH (`x`:`Person`) -/:`likes`{2,}/-> (`y`) WHERE `x`.`name`='Amy'"},
		{`SELECT y.name
  FROM MATCH (x:Person) -/:likes{1,2}/-> (y)
 WHERE x.name = 'Amy'`, true, "SELECT `y`.`name` FROM MATCH (`x`:`Person`) -/:`likes`{1,2}/-> (`y`) WHERE `x`.`name`='Amy'"},
		{`SELECT y.name
  FROM MATCH (x:Person) -/:knows{,2}/-> (y)
 WHERE x.name = 'Judith'`, true, "SELECT `y`.`name` FROM MATCH (`x`:`Person`) -/:`knows`{,2}/-> (`y`) WHERE `x`.`name`='Judith'"},
		{`SELECT src, SUM(e.weight), dst
  FROM MATCH ANY SHORTEST (src) -[e]->* (dst)
 WHERE src.age < dst.age`, true, "SELECT `src`,SUM(`e`.`weight`),`dst` FROM MATCH ANY SHORTEST (`src`) -[`e`]->* (`dst`) WHERE `src`.`age`<`dst`.`age`"},
		{`SELECT src, ARRAY_AGG(e.weight), dst
  FROM MATCH ANY SHORTEST (src) (-[e]-> WHERE e.weight > 10)* (dst)`, true, "SELECT `src`,ARRAY_AGG(`e`.`weight`),`dst` FROM MATCH ANY SHORTEST (`src`) (-[`e`]-> WHERE `e`.`weight`>10)* (`dst`)"},
		{`SELECT src, ARRAY_AGG(e.weight), dst
  FROM MATCH ANY SHORTEST (src) -[e]->* (dst) WHERE SUM(e.cost) < 100`, true, "SELECT `src`,ARRAY_AGG(`e`.`weight`),`dst` FROM MATCH ANY SHORTEST (`src`) -[`e`]->* (`dst`) WHERE SUM(`e`.`cost`)<100"},
		{`SELECT LISTAGG(e.amount, ' + ') || ' = ', SUM(e.amount) AS total_amount
    FROM MATCH ALL SHORTEST (a:Account) -[e:transaction]->* (b:Account)
   WHERE a.number = 10039 AND b.number = 2090
ORDER BY total_amount`, true, "SELECT LISTAGG(`e`.`amount`, ' + ')||' = ',SUM(`e`.`amount`) AS `total_amount` FROM MATCH ALL SHORTEST (`a`:`Account`) -[`e`:`transaction`]->* (`b`:`Account`) WHERE `a`.`number`=10039 AND `b`.`number`=2090 ORDER BY `total_amount`"},
		{`SELECT src, SUM(e.weight), dst
  FROM MATCH TOP 3 SHORTEST (src) -[e]->* (dst)
 WHERE src.age < dst.age`, true, "SELECT `src`,SUM(`e`.`weight`),`dst` FROM MATCH TOP 3 SHORTEST (`src`) -[`e`]->* (`dst`) WHERE `src`.`age`<`dst`.`age`"},
		{`SELECT src, ARRAY_AGG(e.weight), ARRAY_AGG(v1.age), ARRAY_AGG(v2.age), dst
  FROM MATCH TOP 3 SHORTEST (src) ((v1) -[e]-> (v2))* (dst)
 WHERE src.age < dst.age`, true, "SELECT `src`,ARRAY_AGG(`e`.`weight`),ARRAY_AGG(`v1`.`age`),ARRAY_AGG(`v2`.`age`),`dst` FROM MATCH TOP 3 SHORTEST (`src`) ((`v1`) -[`e`]-> (`v2`))* (`dst`) WHERE `src`.`age`<`dst`.`age`"},
		{`SELECT ARRAY_AGG(e1.weight), ARRAY_AGG(e2.weight)
  FROM MATCH (start) -> (src)
     , MATCH TOP 3 SHORTEST (src) (-[e1]->)* (mid)
     , MATCH ANY SHORTEST (mid) (-[e2]->)* (dst)
     , MATCH (dst) -> (end)`, true, "SELECT ARRAY_AGG(`e1`.`weight`),ARRAY_AGG(`e2`.`weight`) FROM MATCH (`start`) -> (`src`),MATCH TOP 3 SHORTEST (`src`) -[`e1`]->* (`mid`),MATCH ANY SHORTEST (`mid`) -[`e2`]->* (`dst`),MATCH (`dst`) -> (`end`)"},
		{`SELECT COUNT(e) AS num_hops
       , SUM(e.amount) AS total_amount
       , ARRAY_AGG(e.amount) AS amounts_along_path
    FROM MATCH TOP 7 SHORTEST (a:Account) -[e:transaction]->* (b:Account)
   WHERE a.number = 10039 AND a = b
ORDER BY num_hops, total_amount`, true, "SELECT COUNT(`e`) AS `num_hops`,SUM(`e`.`amount`) AS `total_amount`,ARRAY_AGG(`e`.`amount`) AS `amounts_along_path` FROM MATCH TOP 7 SHORTEST (`a`:`Account`) -[`e`:`transaction`]->* (`b`:`Account`) WHERE `a`.`number`=10039 AND `a`=`b` ORDER BY `num_hops`,`total_amount`"},
		{`SELECT COUNT(e) AS num_hops
       , SUM(e.amount) AS total_amount
       , ARRAY_AGG(e.amount) AS amounts_along_path
    FROM MATCH TOP 7 SHORTEST (a:Account) -[e:transaction]->* (b:Account)
   WHERE a.number = 10039 AND a = b AND COUNT(DISTINCT e) = COUNT(e) AND COUNT(e) > 0
ORDER BY num_hops, total_amount`, true, "SELECT COUNT(`e`) AS `num_hops`,SUM(`e`.`amount`) AS `total_amount`,ARRAY_AGG(`e`.`amount`) AS `amounts_along_path` FROM MATCH TOP 7 SHORTEST (`a`:`Account`) -[`e`:`transaction`]->* (`b`:`Account`) WHERE `a`.`number`=10039 AND `a`=`b` AND COUNT(DISTINCT `e`)=COUNT(`e`) AND COUNT(`e`)>0 ORDER BY `num_hops`,`total_amount`"},
		{`SELECT COUNT(e) AS num_hops
     , SUM(e.amount) AS total_amount
     , ARRAY_AGG(e.amount) AS amounts_along_path
  FROM MATCH ANY CHEAPEST (a:Account) (-[e:transaction]-> COST e.amount)* (b:Account)
 WHERE a.number = 10039 AND b.number = 2090`, true, "SELECT COUNT(`e`) AS `num_hops`,SUM(`e`.`amount`) AS `total_amount`,ARRAY_AGG(`e`.`amount`) AS `amounts_along_path` FROM MATCH ANY CHEAPEST (`a`:`Account`) (-[`e`:`transaction`]-> COST `e`.`amount`)* (`b`:`Account`) WHERE `a`.`number`=10039 AND `b`.`number`=2090"},
		{`SELECT COUNT(e) AS num_hops
     , SUM(e.amount) AS total_amount
     , ARRAY_AGG(e.amount) AS amounts_along_path
  FROM MATCH ANY CHEAPEST (a:Account) (-[e:transaction]- COST e.amount)* (b:Account)
 WHERE a.number = 10039 AND b.number = 2090`, true, "SELECT COUNT(`e`) AS `num_hops`,SUM(`e`.`amount`) AS `total_amount`,ARRAY_AGG(`e`.`amount`) AS `amounts_along_path` FROM MATCH ANY CHEAPEST (`a`:`Account`) (-[`e`:`transaction`]- COST `e`.`amount`)* (`b`:`Account`) WHERE `a`.`number`=10039 AND `b`.`number`=2090"},
		{`SELECT COUNT(e) AS num_hops
     , SUM(e.amount) AS total_amount
     , ARRAY_AGG(e.amount) AS amounts_along_path
  FROM MATCH ANY CHEAPEST (p1:Person) (-[e:owner|transaction]-
                                      COST CASE
                                             WHEN e.amount IS NULL THEN 1
                                             ELSE e.amount
                                           END)* (p2:Person)
 WHERE p1.name = 'Nikita' AND p2.name = 'Liam'`, true, "SELECT COUNT(`e`) AS `num_hops`,SUM(`e`.`amount`) AS `total_amount`,ARRAY_AGG(`e`.`amount`) AS `amounts_along_path` FROM MATCH ANY CHEAPEST (`p1`:`Person`) (-[`e`:`owner`|`transaction`]- COST CASE WHEN `e`.`amount` IS NULL THEN 1 ELSE `e`.`amount` END)* (`p2`:`Person`) WHERE `p1`.`name`='Nikita' AND `p2`.`name`='Liam'"},
		{`SELECT COUNT(e) AS num_hops
       , SUM(e.amount) AS total_amount
       , ARRAY_AGG(e.amount) AS amounts_along_path
    FROM MATCH TOP 3 CHEAPEST (a:Account) (-[e:transaction]-> COST e.amount)* (a)
   WHERE a.number = 10039
ORDER BY total_amount`, true, "SELECT COUNT(`e`) AS `num_hops`,SUM(`e`.`amount`) AS `total_amount`,ARRAY_AGG(`e`.`amount`) AS `amounts_along_path` FROM MATCH TOP 3 CHEAPEST (`a`:`Account`) (-[`e`:`transaction`]-> COST `e`.`amount`)* (`a`) WHERE `a`.`number`=10039 ORDER BY `total_amount`"},
		{`SELECT COUNT(e) AS num_hops
       , ARRAY_AGG( CASE label(n_x)
                      WHEN 'Person' THEN n_x.name
                      WHEN 'Company' THEN n_x.name
                      WHEN 'Account' THEN CAST(n_x.number AS STRING)
                    END ) AS names_or_numbers
       , SUM( CASE label(n_x) WHEN 'Person' THEN 8 ELSE 1 END ) AS total_cost
    FROM MATCH TOP 4 CHEAPEST
          (a:Account)
            (-[e]- (n_x) COST CASE label(n_x) WHEN 'Person' THEN 3 ELSE 1 END)*
              (c:Company)
   WHERE a.number = 10039 AND c.name = 'Oracle'
ORDER BY total_cost`, true, "SELECT COUNT(`e`) AS `num_hops`,ARRAY_AGG(CASE LABEL(`n_x`) WHEN 'Person' THEN `n_x`.`name` WHEN 'Company' THEN `n_x`.`name` WHEN 'Account' THEN CAST(`n_x`.`number` AS STRING) END) AS `names_or_numbers`,SUM(CASE LABEL(`n_x`) WHEN 'Person' THEN 8 ELSE 1 END) AS `total_cost` FROM MATCH TOP 4 CHEAPEST (`a`:`Account`) (-[`e`]- (`n_x`) COST CASE LABEL(`n_x`) WHEN 'Person' THEN 3 ELSE 1 END)* (`c`:`Company`) WHERE `a`.`number`=10039 AND `c`.`name`='Oracle' ORDER BY `total_cost`"},
		{`SELECT LISTAGG(e.amount, ' + ') || ' = ', SUM(e.amount) AS total_amount
    FROM MATCH ALL (a:Account) -[e:transaction]->{,7} (b:Account)
   WHERE a.number = 10039 AND b.number = 2090
ORDER BY total_amount`, true, "SELECT LISTAGG(`e`.`amount`, ' + ')||' = ',SUM(`e`.`amount`) AS `total_amount` FROM MATCH ALL (`a`:`Account`) -[`e`:`transaction`]->{,7} (`b`:`Account`) WHERE `a`.`number`=10039 AND `b`.`number`=2090 ORDER BY `total_amount`"},
		{`SELECT SUM(COUNT(e)) AS sumOfPathLengths
  FROM MATCH ANY SHORTEST (a:Account) -[e:transaction]->* (b:Account)
 WHERE a.number = 10039 AND (b.number = 1001 OR b.number = 2090)`, true, "SELECT SUM(COUNT(`e`)) AS `sumOfPathLengths` FROM MATCH ANY SHORTEST (`a`:`Account`) -[`e`:`transaction`]->* (`b`:`Account`) WHERE `a`.`number`=10039 AND (`b`.`number`=1001 OR `b`.`number`=2090)"},
		{`SELECT b.number AS b,
         COUNT(e) AS pathLength,
         ARRAY_AGG(e.amount) AS transactions
    FROM MATCH ANY SHORTEST (a:Account) -[e:transaction]->* (b:Account)
   WHERE a.number = 10039 AND
         (b.number = 8021 OR b.number = 1001 OR b.number = 2090) AND
         COUNT(e) <= 2
ORDER BY pathLength`, true, "SELECT `b`.`number` AS `b`,COUNT(`e`) AS `pathLength`,ARRAY_AGG(`e`.`amount`) AS `transactions` FROM MATCH ANY SHORTEST (`a`:`Account`) -[`e`:`transaction`]->* (`b`:`Account`) WHERE `a`.`number`=10039 AND (`b`.`number`=8021 OR `b`.`number`=1001 OR `b`.`number`=2090) AND COUNT(`e`)<=2 ORDER BY `pathLength`"},
		{`SELECT COUNT(e) AS pathLength,
         COUNT(*) AS cnt
    FROM MATCH ANY SHORTEST (a:Account) -[e:transaction]->* (b:Account)
   WHERE (a.number = 10039 OR a.number = 8021) AND
         (b.number = 1001 OR b.number = 2090)
GROUP BY COUNT(e)
ORDER BY pathLength`, true, "SELECT COUNT(`e`) AS `pathLength`,COUNT(1) AS `cnt` FROM MATCH ANY SHORTEST (`a`:`Account`) -[`e`:`transaction`]->* (`b`:`Account`) WHERE (`a`.`number`=10039 OR `a`.`number`=8021) AND (`b`.`number`=1001 OR `b`.`number`=2090) GROUP BY COUNT(`e`) ORDER BY `pathLength`"},
		{`SELECT fof.name, COUNT(friend) AS num_common_friends
  FROM MATCH (p:Person) -[:has_friend]-> (friend:Person) -[:has_friend]-> (fof:Person)
 WHERE NOT EXISTS ( SELECT * FROM MATCH (p) -[:has_friend]-> (fof) )`, true, "SELECT `fof`.`name`,COUNT(`friend`) AS `num_common_friends` FROM MATCH (`p`:`Person`) -[:`has_friend`]-> (`friend`:`Person`) -[:`has_friend`]-> (`fof`:`Person`) WHERE NOT EXISTS (SELECT * FROM MATCH (`p`) -[:`has_friend`]-> (`fof`))"},
		{`SELECT a.name
  FROM MATCH (a)
 WHERE a.age > ( SELECT AVG(b.age) FROM MATCH (a) -[:friendOf]-> (b) )`, true, "SELECT `a`.`name` FROM MATCH (`a`) WHERE `a`.`age`>(SELECT AVG(`b`.`age`) FROM MATCH (`a`) -[:`friendOf`]-> (`b`))"},
		{`SELECT p.name AS name
       , ( SELECT SUM(t.amount)
             FROM MATCH (a) <-[t:transaction]- (:Account)
                     ON financial_transactions
         ) AS sum_incoming
       , ( SELECT SUM(t.amount)
             FROM MATCH (a) -[t:transaction]-> (:Account)
                     ON financial_transactions
         ) AS sum_outgoing
       , ( SELECT COUNT(DISTINCT p2)
             FROM MATCH (a) -[t:transaction]- (:Account) -[:owner]-> (p2:Person)
                     ON financial_transactions
            WHERE p2 <> p
         ) AS num_persons_transacted_with
       , ( SELECT COUNT(DISTINCT c)
             FROM MATCH (a) -[t:transaction]- (:Account) -[:owner]-> (c:Company)
                     ON financial_transactions
         ) AS num_companies_transacted_with
    FROM MATCH (p:Person) <-[:owner]- (a:Account) ON financial_transactions
ORDER BY sum_outgoing + sum_incoming DESC`, true, "SELECT `p`.`name` AS `name`,(SELECT SUM(`t`.`amount`) FROM MATCH (`a`) <-[`t`:`transaction`]- (:`Account`) ON `financial_transactions`) AS `sum_incoming`,(SELECT SUM(`t`.`amount`) FROM MATCH (`a`) -[`t`:`transaction`]-> (:`Account`) ON `financial_transactions`) AS `sum_outgoing`,(SELECT COUNT(DISTINCT `p2`) FROM MATCH (`a`) -[`t`:`transaction`]- (:`Account`) -[:`owner`]-> (`p2`:`Person`) ON `financial_transactions` WHERE `p2`<>`p`) AS `num_persons_transacted_with`,(SELECT COUNT(DISTINCT `c`) FROM MATCH (`a`) -[`t`:`transaction`]- (:`Account`) -[:`owner`]-> (`c`:`Company`) ON `financial_transactions`) AS `num_companies_transacted_with` FROM MATCH (`p`:`Person`) <-[:`owner`]- (`a`:`Account`) ON `financial_transactions` ORDER BY `sum_outgoing`+`sum_incoming` DESC"},
		{`SELECT p.name AS name
       , ( SELECT SUM(t.amount)
             FROM MATCH (a) <-[t:transaction]- (:Account)
         ) AS sum_incoming
       , ( SELECT SUM(t.amount)
             FROM MATCH (a) -[t:transaction]-> (:Account)
         ) AS sum_outgoing
       , ( SELECT COUNT(DISTINCT p2)
             FROM MATCH (a) -[t:transaction]- (:Account) -[:owner]-> (p2:Person)
            WHERE p2 <> p
         ) AS num_persons_transacted_with
       , ( SELECT COUNT(DISTINCT c)
             FROM MATCH (a) -[t:transaction]- (:Account) -[:owner]-> (c:Company)
         ) AS num_companies_transacted_with
    FROM MATCH (p:Person) <-[:owner]- (a:Account)
ORDER BY sum_outgoing + sum_incoming DESC`, true, "SELECT `p`.`name` AS `name`,(SELECT SUM(`t`.`amount`) FROM MATCH (`a`) <-[`t`:`transaction`]- (:`Account`)) AS `sum_incoming`,(SELECT SUM(`t`.`amount`) FROM MATCH (`a`) -[`t`:`transaction`]-> (:`Account`)) AS `sum_outgoing`,(SELECT COUNT(DISTINCT `p2`) FROM MATCH (`a`) -[`t`:`transaction`]- (:`Account`) -[:`owner`]-> (`p2`:`Person`) WHERE `p2`<>`p`) AS `num_persons_transacted_with`,(SELECT COUNT(DISTINCT `c`) FROM MATCH (`a`) -[`t`:`transaction`]- (:`Account`) -[:`owner`]-> (`c`:`Company`)) AS `num_companies_transacted_with` FROM MATCH (`p`:`Person`) <-[:`owner`]- (`a`:`Account`) ORDER BY `sum_outgoing`+`sum_incoming` DESC"},
		{`PATH has_parent AS () -[:has_father|has_mother]-> (:Person)
SELECT ancestor.name
  FROM MATCH (p1:Person) -/:has_parent+/-> (ancestor)
     , MATCH (p2:Person) -/:has_parent+/-> (ancestor)
 WHERE p1.name = 'Mario'
   AND p2.name = 'Luigi'`, true, "PATH `has_parent` AS () -[:`has_father`|`has_mother`]-> (:`Person`) SELECT `ancestor`.`name` FROM MATCH (`p1`:`Person`) -/:`has_parent`+/-> (`ancestor`),MATCH (`p2`:`Person`) -/:`has_parent`+/-> (`ancestor`) WHERE `p1`.`name`='Mario' AND `p2`.`name`='Luigi'"},
		{`PATH connects_to AS (:Generator) -[:has_connector]-> (c:Connector) <-[:has_connector]- (:Generator)
                WHERE c.status = 'OPERATIONAL'
SELECT generatorA.location, generatorB.location
  FROM MATCH (generatorA) -/:connects_to+/-> (generatorB)`, true, "PATH `connects_to` AS (:`Generator`) -[:`has_connector`]-> (`c`:`Connector`) <-[:`has_connector`]- (:`Generator`) WHERE `c`.`status`='OPERATIONAL' SELECT `generatorA`.`location`,`generatorB`.`location` FROM MATCH (`generatorA`) -/:`connects_to`+/-> (`generatorB`)"},
		{`PATH macro1 AS (v1:Generator) -[e1:has_connector]-> (v2:Connector)
SELECT COUNT(*)
FROM MATCH (generatorA) <-/:macro1+/- (generatorB)
WHERE generatorA.name = 'AEH382'`, true, "PATH `macro1` AS (`v1`:`Generator`) -[`e1`:`has_connector`]-> (`v2`:`Connector`) SELECT COUNT(1) FROM MATCH (`generatorA`) <-/:`macro1`+/- (`generatorB`) WHERE `generatorA`.`name`='AEH382'"},
		{`PATH macro1 AS (v2:Connector) <-[e1:has_connector]- (v1:Generator)
SELECT COUNT(*)
FROM MATCH (generatorA) -/:macro1+/-> (generatorB)
WHERE generatorA.name = 'AEH382'`, true, "PATH `macro1` AS (`v2`:`Connector`) <-[`e1`:`has_connector`]- (`v1`:`Generator`) SELECT COUNT(1) FROM MATCH (`generatorA`) -/:`macro1`+/-> (`generatorB`) WHERE `generatorA`.`name`='AEH382'"},
	}
	RunTest(t, table)
}

func RunTest(t *testing.T, table []testCase) {
	p := parser.New()
	for _, tbl := range table {
		_, _, err := p.Parse(tbl.src)
		if !tbl.ok {
			require.Errorf(t, err, "source %v", tbl.src)
			continue
		}
		require.NoErrorf(t, err, "source %v", tbl.src)
		// restore correctness test
		if tbl.ok {
			RunRestoreTest(t, tbl.src, tbl.restore)
		}
	}
}

func RunRestoreTest(t *testing.T, sourceSQLs, expectSQLs string) {
	var sb strings.Builder
	p := parser.New()
	comment := fmt.Sprintf("source %v", sourceSQLs)
	stmts, _, err := p.Parse(sourceSQLs)
	require.NoErrorf(t, err, "source %v", sourceSQLs)
	restoreSQLs := ""
	for _, stmt := range stmts {
		sb.Reset()
		err = stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
		require.NoError(t, err, comment)
		restoreSQL := sb.String()
		comment = fmt.Sprintf("source %v; restore %v", sourceSQLs, restoreSQL)
		restoreStmt, err := p.ParseOneStmt(restoreSQL)
		require.NoError(t, err, comment)
		_ = restoreStmt
		// require.Equal(t, stmt, restoreStmt, comment) // TODO: compare stmt and restoreStmt
		if restoreSQLs != "" {
			restoreSQLs += "; "
		}
		restoreSQLs += restoreSQL

	}
	require.Equalf(t, expectSQLs, restoreSQLs, "restore %v; expect %v", restoreSQLs, expectSQLs)
}
