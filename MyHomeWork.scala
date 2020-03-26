package ru.philit.bigdata.vsu.scalaLang.oop.MyHome
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A

import scala.annotation.tailrec
import scala.util.Try
import scala.xml.Null

object MyHomeWork extends App {
  //  1) Найти K-тый элемент в списке
  val seq = Seq(1,2,3,4,5,6)
  val k: Int = 5
  println("Получили k - элемент - " + Try(seq(k)).getOrElse("никакой"))

  @tailrec
  def tailRecFind(k: Int, seq: Seq[Int]): Int =
    k match
    {
      case 0 => seq.head
      case k if k > 0 => tailRecFind(k - 1, seq.tail)
      case _ => throw new Exception
    }

  println("Получили k - элемент - " + tailRecFind(k, seq))

  //  2) Получить число элементов списка без использования size и length
  val seqNum = List(1,2,3,4,5,6)
  def lengthN[A](seq: List[A]) = {
    @tailrec
    def myLength[A](n: Int, list: List[A]): Int =
      list match {
        case Nil => n
        case _ => myLength(n + 1, list.tail)
      }
    println("Количесво элементов в списке: " + myLength(0, seq))
  }
  lengthN(seqNum)
  //  3) Написать функцию проверки последовательности на палиндромность (ex: 1 2 3 2 1)
  val seqPalindrom = Seq(1,2,3,2,1)

  def isPalindromNum[A](seq: Seq[A]): Boolean = seq == seq.reverse

  println("Последовательность палиндромна? - " + isPalindromNum(seqPalindrom))
  //  4) Написать функцию distinct по удалению дублей из последовательности. Set и аналоги использовать нельзя

  def distinct(list: List[Int]): List[Int] =
    list.sortWith(_ < _)
      .foldRight(List[Int]())
  {
    case (element, result) if(result.isEmpty || result.head != element) => element::result
    case (element, result) => result
  }
  val listLs = List(1,2,3,3,4,3,5,6)

  println("Список без лишнего: " + distinct(listLs))

  //  //5) Написать distinctPack дающий результат вида (уникальная последовательность, список с вложенными списками из дубликатов)
  //  //(ex: input - [1, 3, 3, 2, 1] -> ([[1, 1], [3, 3], [2]]))
  def distinctPack(list: List[Int]): List[List[Int]] =
  {
    @tailrec
    def distinctP(twiceList: List[List[Int]], list: List[Int]): List[List[Int]] =
      list.sortWith(_ < _) match {
        case Nil => twiceList
        case element :: tail if twiceList.isEmpty || twiceList.last.head != element => distinctP(twiceList ::: List(List(element)), tail)
        case element :: tail => distinctP(twiceList.init ::: List(twiceList.last ::: List(element)), tail)
      }

    distinctP(List(), list)
  }
  val list = List(1,2,3,4,5,5,5,6,5,6,5,7,7,5,4,6,5)
  println("Комплекты списков: " + distinctPack(list))


  //  6) Написать собственную реализацию slice(Option[StartPos], Option[EndPos])
  val seqR = Seq(1,2,3,4,5,6)
 // println(seqR.slice(-1, -5))
  def slice(startPos: Int, endPos: Int, seq: Seq[Int]): Seq[Int] = {
  @tailrec
    def sliceCopy(startPos: Int, endPos: Int, seq: Seq[Int]): Seq[Int] =
      seq match {
        case sq if endPos == 0 && startPos == 0 => sq
        case sq if endPos > 0 => sliceCopy(startPos, endPos - 1, sq.init)
        case h :: tail if startPos > 0 => sliceCopy(startPos - 1, endPos, tail)
      }
    if (startPos >= endPos || startPos < 0 || endPos < 0 || endPos > seq.length) List()
    else sliceCopy(startPos, seq.length - endPos, seq)
  }

  println("Slice: " + slice(1,2,seqR))


  //  7) Написать функцию создающую из заданой последовательности (любого типа)
  //  все возможные группы по N и только уникальные. (1, 2) и (2, 1) считаются одинаковыми комбинациями.

  val listLsTwo = List(1,2,3,3,4,3,5,6)
  val distance: Int = 4
  def complexNumber(list: List[Int], n: Int) =
  {
    @tailrec
    def myRecTailNset(twiceList: List[List[Int]], list: List[Int], n: Int, k: Int): List[List[Int]] =
      list match
      {
        case Nil => twiceList
        case el :: tail if twiceList.isEmpty => myRecTailNset(twiceList ::: List(List(el)), tail, n - 1, k)
        case el :: tail if n == 0 => myRecTailNset(twiceList ::: List(List(el)), tail, k - 1, k)
        case el :: tail if n > 0 => myRecTailNset(twiceList.init ::: List(twiceList.last ::: List(el)), tail, n - 1, k)

      }
      if (n > list.length || n < 1)
      {
        myRecTailNset(List(),distinct(list), list.length, list.length)
      }
      else myRecTailNset(List(),distinct(list), n, n)
  }

  println("Последовательности: " + complexNumber(listLsTwo, distance))

  //  8) Написать функцию выводящую только простые числа из заданого диапазона(Range)

  val Range = Seq(0,6)
  val vector = Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
  def primeNumber(vector: Vector[Int], Range: Seq[Int]): Unit=
  {
    val seqRange = vector.zipWithIndex.flatMap {
      case (el, index)
        if !(2 until el).exists(el % _ == 0)
          && index >= Range.head
          && index <= Range.last && el != 1 => Seq(el)
      case _ => Null
    }
    seqRange.foreach(x => print(x + " "))
  }
  print("Последовательность простых чисел из заданного диапазона: ")
  primeNumber(vector, Range)

}




