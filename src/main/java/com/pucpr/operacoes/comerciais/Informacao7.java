/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pucpr.operacoes.comerciais;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author lucas.francisconi
 */
public class Informacao7 {
    
    public static class MapperInformacao7 extends Mapper<Object, Text, Text, IntWritable> {
        
       @Override
        protected void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            String linha = valor.toString();
            String[] valores = linha.split(";");
            
            if(valores.length == 10) {
                String mercadoria = valores[3];
                int peso = Integer.valueOf(valores[6]);
            
                Text chaveMap = new Text(mercadoria);
                IntWritable valorMap = new IntWritable(peso);
                
                context.write(chaveMap, valorMap);
            }
        }
    }
    
    public static class ReducerInformacao7 extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private Text mercadoria;
        private IntWritable totalPeso;
        
        @Override
        protected void setup(Context context) {
            mercadoria = new Text("");
            totalPeso = new IntWritable(0);
        }
        
        @Override
        protected void reduce(Text chave, Iterable<IntWritable> valores, Context context) {
            Integer soma = 0;
            
            for(IntWritable valor : valores) {
                soma += valor.get();
            }
            
            if(soma > totalPeso.get()) {
                totalPeso.set(soma);
                mercadoria.set(chave.toString());
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(mercadoria, totalPeso);
        }
    
    }

    public static void main (String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
          String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
          String arquivoSaida = "/home2/ead2022/SEM1/lucas.francisconi/Desktop/atp/etapa2/informacao7";
        
          if(args.length == 2) {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
          }
        
          Configuration conf = new Configuration();
          Job job = Job.getInstance(conf, "etapa02-atv07");
        
          job.setJarByClass(Informacao7.class);
          job.setMapperClass(MapperInformacao7.class);
          job.setReducerClass(ReducerInformacao7.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(IntWritable.class);
        
          FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
          FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
          job.waitForCompletion(true);
    }
}
