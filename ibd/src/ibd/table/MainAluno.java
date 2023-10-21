/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table;

import ibd.table.record.Record;
import java.util.Iterator;

/**
 *
 * @author Sergio
 */
public class MainAluno {

    public void addAluno(Table table, long pk, String nome, int matricula) throws Exception {

    }

    public String buildAluno(int matricula, String nome) {
        return "";

    }

    public String getNome(String aluno) {
        return "";
    }

    public String getMatricula(String aluno) {
        return "";
    }

    public static void main(String[] args) throws Exception {

        MainAluno main = new MainAluno();

        Table table = Directory.getTable("c:\\teste\\ibd", "aluno", 4096, true);

        main.addAluno(table, 1, "Ana", 11);
        main.addAluno(table, 2, "Zeca", 12);
        main.addAluno(table, 3, "Joao", 13);

        table.flushDB();

        Iterator<Record> i = table.iterator();
        while (i.hasNext()) {
            Record r = i.next();
            String content = r.getContent();
            String matr = main.getMatricula(content);
            String nome = main.getNome(content);
            System.out.println("matricula:" + matr + " nome:" + nome);
        }

    }

}
