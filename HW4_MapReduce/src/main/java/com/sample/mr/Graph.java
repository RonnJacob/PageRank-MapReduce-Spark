package com.sample.mr;
import java.io.*;
import java.util.ArrayList;


class Vertex {
    private Integer vertex_no;
    private Double rank;
    private ArrayList<Integer> connected_vertices;
    private Boolean is_dangling;
    Vertex(){
        this.vertex_no = null;
        this.rank = 1.0;
        this.connected_vertices = new ArrayList<Integer>();
        this.is_dangling = true;
    }
    Vertex(Integer vertex_no, Double rank, Boolean is_dangling){
        this.vertex_no = vertex_no;
        this.rank = rank;
        this.connected_vertices = new ArrayList<Integer>();
        this.is_dangling = is_dangling;
    }

    public Integer getVertex_no() {
        return vertex_no;
    }

    public void setVertex_no(Integer vertex_no) {
        this.vertex_no = vertex_no;
    }

    public ArrayList<Integer> getConnected_vertices() {
        return connected_vertices;
    }

    public void setConnected_vertices(ArrayList<Integer> connected_vertices) {
        this.connected_vertices = connected_vertices;
    }

    public Boolean getIs_dangling() {
        return is_dangling;
    }

    public void setIs_dangling(Boolean is_dangling) {
        this.is_dangling = is_dangling;
    }

    public Double getRank() {
        return rank;
    }

    public void setRank(Double rank) {
        this.rank = rank;
    }

    public String encodeVertexInfo(){
        return "#"+this.getVertex_no().toString()+"%"+this.getRank().toString()+"v"
                +this.getConnected_vertices().toString();
    }

    public Vertex decodeVertexInfo(String vertexInfo){
        String value = vertexInfo.trim();
        String[] values = vertexInfo.split("%");
        this.vertex_no = Integer.parseInt(values[0].substring(1));
        this.rank = Double.parseDouble(values[1].split("v")[0]);
        String[] neighbours = values[1].split("v")[1].replaceAll("[\\[\\]]","").split(",");
        ArrayList<Integer> adjList = new ArrayList<>();
        for(String v: neighbours){
            adjList.add(Integer.parseInt(v));
        }
        this.connected_vertices = adjList;
        return this;
    }
}

class GenerateGraph{
    private Integer k;
    private ArrayList<Vertex> vertices;
    GenerateGraph(Integer k){
        this.k = k;
        this.vertices = new ArrayList<>();
    }

    public ArrayList<Vertex> graphGenerator(){
        Double init_pr = this.k.doubleValue()*this.k.doubleValue();
        for(Integer i = 1; i <= this.k * this.k ; i++){
            if(i % this.k != 0){
                Vertex vtx = new Vertex(i, 1/init_pr, false);
                ArrayList<Integer> vtx_vertices = vtx.getConnected_vertices();
                vtx_vertices.add(i+1);
                vtx.setConnected_vertices(vtx_vertices);
                this.vertices.add(vtx);
            }
            else{
                Vertex vtx = new Vertex(i, 1/init_pr, false);
                ArrayList<Integer> vtx_vertices = vtx.getConnected_vertices();
                vtx_vertices.add(0);
                vtx.setConnected_vertices(vtx_vertices);
                this.vertices.add(vtx);
            }
        }
        return this.vertices;
    }

    public ArrayList<Vertex> getVertices() {
        return vertices;
    }

    public void setVertices(ArrayList<Vertex> vertices) {
        this.vertices = vertices;
    }

    public Integer getK() {
        return k;
    }

    public void setK(Integer k) {
        this.k = k;
    }



    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        GenerateGraph graph = new GenerateGraph(100);
        ArrayList<Vertex> vertices = graph.graphGenerator();
        PrintWriter writer = new PrintWriter("input/input.txt", "UTF-8");

        for(Vertex v: vertices){
            String vertexInfo = v.getVertex_no().toString() + "," + v.getConnected_vertices().get(0).toString();
            writer.println(vertexInfo);
        }
        writer.close();
    }
}