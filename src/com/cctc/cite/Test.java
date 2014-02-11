package com.cctc.cite;

public class Test {
	public static class Node{
		private int a;
		private int b;
		public Node(int c,int d){
			this.a = c;
			this.b = d;
		}
	}
	public static void main(String[] args){
		Node[] n = new Node[2];
		Node i = new Node(1,2);
		Node j = new Node(0,9);
		n[0] = i;
		n[1] = j;
		n[0].a = 6;
		System.out.println(i.a);
		System.out.println(n[0].a);
	}
}
