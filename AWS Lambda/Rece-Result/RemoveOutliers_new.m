% 函数用于去除离群点和返回索引值
function [mean_value, indices] = RemoveOutliers_new(data)
    % 将数据转换为列向量
    data = data(:);
    n = length(data);
    
    % 初始均值和标准差
    mean_value = mean(data);
    std_value = std(data);
    
    % 创建一个初始的索引数组
    indices = 1:n;
    
    % 设置最大循环次数
    max_iterations = 10;
    iteration_count = 0;
    
    % 从两端向中间去除离群点
    while true
        % 更新循环计数器
        iteration_count = iteration_count + 1;
        
        % 计算与当前均值的差距
        diff = abs(data - mean_value);
        
        % 找到最大差距的索引
        [~, outlier_index] = max(diff);
        
        % 判断是否超过一定数量的标准差
        if diff(outlier_index) <= 3 * std_value || iteration_count > max_iterations
            break;
        end
        
        % 从左侧或右侧删除数据点，直到索引超过当前离群点
        if outlier_index <= length(data) / 2
            % 左侧删除数据点
            data = data(outlier_index:end);
            indices = indices(outlier_index:end);
        else
            % 右侧删除数据点
            data = data(1:outlier_index);
            indices = indices(1:outlier_index);
        end
        
        % 更新均值和标准差
        mean_value = mean(data);
        std_value = std(data);
        
        % 更新n
        n = length(data);
        
        % 检查是否剩余的数据点过少
        if n <= 1
            break;
        end
    end
end
