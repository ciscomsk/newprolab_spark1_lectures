0 % 10
1 % 10
2 % 10
3 % 10
4 % 10
5 % 10
6 % 10
7 % 10
8 % 10
9 % 10

9.99.round

10 % 3
-10 % 3

def pmod(a: Int, n: Int): Int = {
  val r: Int = a % n
  if (r < 0) {
    (r + n) % n
  } else r
}

pmod(10, 3)
pmod(-10, 3)