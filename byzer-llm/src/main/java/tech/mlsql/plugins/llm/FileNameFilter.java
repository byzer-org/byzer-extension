package tech.mlsql.plugins.llm;

/**
 * 4/4/23 WilliamZhu(allwefantasy@gmail.com)
 */
public interface FileNameFilter {
    boolean accept(String fileName);
}
