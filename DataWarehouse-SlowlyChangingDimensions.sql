create procedure dbo.Dim_Type2_GenerateCode
    @TableNameSource varchar(255),
    @TableSourceLoadDate varchar(255) = 'LoadDatetime',

    @NaturalKey varchar(255),
    @NaturalBeginDateChange varchar(255),
    @NaturalEndDateChange varchar(255) = null,

    @RepeatedDataFlag bit = 0,
    @TempTableFlag bit = 0,
    @ColumnExcludeList varchar(max) = null

as

set nocount on

-- Purpose:  Generate slowly changing dimensions for any table
-- Note:
-- In case of Column Renames or joins between Staging and Dimension, utilize MapVw, eg FoodMapVw, CustomerMapVw, ProductMapVw
-- To Include only certain columns between Staging and Dimension, utilize MapVw, eg FoodMapVw, CustomerMapVw, ProductMapVw
-- To Load data into smaller temp table to prevent querying repeatedly, utilize @TempTableFlag = 1
-- Column Exclusion parameter can be added to exclude certain columns
-- Works with three table types below: Transaction Dates, Tables with Beg/EndDates already, Tables with RepeatedData

--      ProductId ProductName  TransactionDate      ProductId    ProductName   TransactionDate  EndEffDate           ProductId     ProductName   TransactionDate (Repeated)
--         1      Apple         4/1/2018              1             Apple         4/1/2018       4/3/2018               1              Apple           4/1/2018
--         1      Apple         4/3/2018              1             Apple         4/3/2018       4/7/2018               1              Apple           4/1/2018
--         1      Apple         4/7/2018              1             Apple         4/12/2018      12/31/999              1              Apple           4/2/2018
--                                                                                                                      1              Apple           4/3/2018

set @TableNameSource = right(@TableNameSource, len(@TableNameSource) - charindex('.', @TableNameSource))
declare @StageTableName varchar(255)
declare @DimTableName varchar(255)
set @TempTableFlag = (case when @NaturalBeginDateChange is null or @NaturalEndDateChange is null or @RepeatedDataFlag = 1 then 1 else 0 end)



DECLARE @ColumnExcludeTable table(ColumnExcludeValue varchar(500) not null);
insert into @ColumnExcludeTable(ColumnExcludeValue)
select ltrim(rtrim(value)) as ColumnExcludeValue from string_split(@ColumnExcludeList, ',');



declare @TempTableDeclare varchar(max) = 'create table #'+@TableNameSource+
'
( 
    ' + @TableNameSource + '_id bigint primary key identity(1,1),' + 
    (select STUFF((
    SELECT ', 
    '    
    + c.name + ' ' + 
case 
    when t.name like '%char%' then t.name + '(' + cast(c.max_length as varchar(10)) + ')' 
    when t.name like '%numeric%' or t.name like '%decimal%' then t.name + '(' + cast(c.precision as varchar(10)) + ',' + cast(c.scale as varchar(10)) + ')'
    else t.name
end
FROM .sys.columns c 
inner JOIN sys.types t
    on t.user_type_id = c.user_type_id
    and t.system_type_id = c.system_type_id
where c.object_id = object_id(@TableNameSource) and is_identity = 0
    and c.name not like '%@NaturalKey%' 
    and c.name not like '%EndEffDate%' 
    and c.name <> @NaturalBeginDateChange
    and c.name not in (select columnexcludetable.ColumnExcludeValue from @ColumnExcludeTable columnexcludetable)
FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,''))
+ ' 
    ,BegEffDatetime datetime
    ,EndEffDatetime datetime
)'  


declare @TempTableCodeInsert varchar(max)=
    (select STUFF((
    SELECT ', 
    '    
    + QUOTENAME(c.name) 
FROM .sys.columns c 
inner JOIN sys.types t
    on t.user_type_id = c.user_type_id
    and t.system_type_id = c.system_type_id
where c.object_id = object_id(@TableNameSource) and is_identity = 0
    and c.name not like '%BegEffDate%' 
    and c.name not like '%EndEffDate%' 
    and c.name <> @NaturalBeginDateChange
    and c.name not in (select columnexcludetable.ColumnExcludeValue from @ColumnExcludeTable columnexcludetable)
FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,''))

declare @TempTableCodeInsertNoLoadDate varchar(max) = (replace(@TempTableCodeInsert,quotename(@TableSourceLoadDate),''))
set @TempTableCodeInsertNoLoadDate = LEFT(@TempTableCodeInsertNoLoadDate, LEN(@TempTableCodeInsertNoLoadDate)-5)
--set @TempTableCodeInsertNoLoadDate = (replace(@TempTableCodeInsertNoLoadDate,quotename(@NaturalBeginDateChange),''))


declare @ColumnListNoPrimary varchar(max) = 
    (select STUFF((
    SELECT ', 
        '    
    + QUOTENAME(c.name) 
    FROM .sys.columns c 
    where c.object_id = object_id(@TableNameSource) and is_identity = 0
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,''))


declare @ColumnListNoPrimaryNoTimeStamp varchar(max) = 
    (select STUFF((
    SELECT ', 
        '    
    + QUOTENAME(c.name) 
    FROM .sys.columns c 
    where c.object_id = object_id(@TableNameSource) and is_identity = 0
    and c.name <> @NaturalKey
    and c.name not like '%BeginEffDate%' 
    and c.name not like '%EndEffDate%' 
    and c.name not like '%CreateDatetime%' 
    and c.name not like '%UpdateDatetime%'
    and c.name not like '%Loaddate%'
    and c.name <> @NaturalBeginDateChange
    and c.name not in (select columnexcludetable.ColumnExcludeValue from @ColumnExcludeTable columnexcludetable)
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,''))



declare @ColumnListNoPrimaryCompareCheck varchar(max) = 
    (select STUFF((
    SELECT ' or
        '    
    + 'stg.'+ QUOTENAME(c.name) + ' <> dim.' + QUOTENAME(c.name) 
    FROM .sys.columns c 
    where c.object_id = object_id(@TableNameSource) and is_identity = 0
    and c.name <> @NaturalKey
    and c.name not like '%BeginEffDate%' 
    and c.name not like '%EndEffDate%' 
    and c.name not like '%CreateDatetime%' 
    and c.name not like '%UpdateDatetime%'
    and c.name not like '%Loaddate%'
    and c.name <> @NaturalBeginDateChange
    and c.name not in (select columnexcludetable.ColumnExcludeValue from @ColumnExcludeTable columnexcludetable)
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,3,''))



set @StageTableName = REPLACE(@TableNameSource,'Dim','Stage')
set @StageTableName = (case when @TempTableFlag = 1 then '#' + @StageTableName else 'dbo.' + @StageTableName end)

set @DimTableName = REPLACE(@TableNameSource,'Stage','Dim')
set @DimTableName = REPLACE(@DimTableName,'MapVw','')
set @DimTableName = (case when left(@DimTableName,3) <> 'Dim' then 'Dim_' + @DimTableName else @DimTableName end)

set @NaturalBeginDateChange = (case when @TempTableFlag = 1 then @NaturalBeginDateChange  else @NaturalBeginDateChange end)
declare @NaturalBeginDateChangeRename varchar(255) = (case when @TempTableFlag = 1 then 'BegEffDatetime'  else @NaturalBeginDateChange end)



declare @TableListGenerateCode nvarchar(max) = 

'create procedure dbo.' + @DimTableName +'Update
    @LoadDatetimeParam datetime
as

declare @NewDatetime datetime = getdate() 

'
+ 

'--Declare temp data
' + 
case when @TempTableFlag = 1 

then 
+ @TempTableDeclare + '

insert into ' + @StageTableName + 
'
(       
' + @TempTableCodeInsert + '
    ,BegEffDatetime
    ,EndEffDatetime
)' + 
case when @RepeatedDataFlag = 1
then 
'select 
    ' + replace(@TempTableCodeInsert,quotename(@TableSourceLoadDate),'min('+quotename(@TableSourceLoadDate)+')') + '
    ,min(' + @NaturalBeginDateChange + ') as BegEffDatetime  
    ,case when max(stg.' + @NaturalBeginDateChange + ') = (select max(substage.' + @NaturalBeginDateChange + ') from ' + @TableNameSource + ' substage where stg.'+ @NaturalKey + ' = substage.' + @NaturalKey + ') then ''12/31/9999'' else max(stg.' + @NaturalBeginDateChange + ') end as EndDate
from ' + @TableNameSource + ' stg
where LoadDatetime > @LoadDatetimeParam
group by ' + @TempTableCodeInsertNoLoaddate
when @RepeatedDataFlag = 0 
then
'
select
    distinct'
+  @TempTableCodeInsert + '
    ,' + @NaturalBeginDateChange + ' as BegEffDatetime
    ,ISNULL(Lead(' + @NaturalBeginDateChange + ' + 1) Over (partition by ' + @NaturalKey + ' order by ' + @NaturalBeginDateChange + ' asc), ''12/31/9999'') as EndEffDatetime'


+ '
from ' + @TableNameSource +
'
where LoadDatetime > @LoadDatetimeParam'
end
else 
''
end 
+ ' 
--End temp data section '  + '


--Begin Transaction

begin transaction

    --Close off existing records that have changed

    update dbo.' + @DimTableName + '
    set
        UpdateDatetime = @NewDatetime,
        EndEffDatetime = (select min(' + @NaturalBeginDateChangeRename + ') from ' + @StageTableName +' substage where stg.' + @NaturalKey + ' = substage.' + @NaturalKey + ')
    from dbo.' + @DimTableName + ' dim
    inner join ' + @StageTableName + ' stg
        on dim.' + @NaturalKey + ' = stg.' + @NaturalKey + '
        and dim.EndEffDatetime = ''12/31/9999''
    where 
    stg.LoadDatetime > @LoadDatetimeParam
    and ('
    + @ColumnListNoPrimaryCompareCheck +


    ')
    --Insert new updated records

    insert into dbo.' + @DimTableName + 
    '
    (   
        ' + @NaturalKey  + ',
        ' + @ColumnListNoPrimaryNoTimeStamp + ',' +
        '
        [CreateDatetime],
        [UpdateDatetime],
        [BegEffDatetime],
        [EndEffDatetime]
    )
    select  
        stg.' + @NaturalKey +',' 
        + replace(@ColumnListNoPrimaryNoTimeStamp,'[','stg.[') + 
        '
        ,@newdatetime as CreateDatetime
        ,@newdatetime as UpdateDatetime
        ,stg.BegEffDatetime as BegEffDatetime
        ,stg.EndEffDatetime as EndEffDatetime
    from '  + @StageTableName + ' stg
    inner join ' + @DimTableName + ' dim
        on dim.' + @NaturalKey + ' = stg.' + @NaturalKey + '
        and dim.UpdateDatetime = @NewDatetime
    --Verify Updates
    where 
        stg.LoadDatetime > @LoadDatetimeParam
        and ('
    + @ColumnListNoPrimaryCompareCheck + ')

    --Insert New Business Key records which do not exist

    UNION ALL
        select
        stg.' + @NaturalKey +',
        ' + replace(@ColumnListNoPrimaryNoTimeStamp,'[','stg.[') + 
        '
        ,@newdatetime as CreateDatetime
        ,@newdatetime as UpdateDatetime
        ,stg.BegEffDatetime as BegEffDatetime
        ,''12/31/9999'' as EndEffDate
    from '  + @StageTableName + ' stg
    left join dbo.' + @DimTableName + ' dim
        on dim.' + @NaturalKey + ' = stg.' + @NaturalKey + '
    where dim.' + @NaturalKey + ' is null   

commit transaction

--end code'

-- Print columns in three steps, print and select only can print first 8000 characters
print substring(@TableListGenerateCode,charindex('--Create procedure',@TableListGenerateCode),charindex('--Declare temp data',@TableListGenerateCode))
print substring(@TableListGenerateCode,charindex('--Declare temp data',@TableListGenerateCode),charindex('--End temp data',@TableListGenerateCode)-charindex('--Declare temp data',@TableListGenerateCode))
print substring(@TableListGenerateCode,charindex('--Begin transaction',@TableListGenerateCode),charindex('--Close off existing records',@TableListGenerateCode)-charindex('--Begin Transaction',@TableListGenerateCode))
print  '    ' + substring(@TableListGenerateCode,charindex('--Close off existing records',@TableListGenerateCode),charindex('--Insert new updated records',@TableListGenerateCode)-charindex('--Close off existing records',@TableListGenerateCode))
print  '    ' + substring(@TableListGenerateCode,charindex('--Insert new updated records',@TableListGenerateCode),charindex('--Verify updates',@TableListGenerateCode) -charindex('--Insert new updated records',@TableListGenerateCode) )
print  '    ' + substring(@TableListGenerateCode,charindex('--Verify updates',@TableListGenerateCode),charindex('--Insert New Business',@TableListGenerateCode) -charindex('--Verify updates',@TableListGenerateCode) )
print  '    ' + substring(@TableListGenerateCode,charindex('--Insert New Business',@TableListGenerateCode),charindex('--end code',@TableListGenerateCode)-charindex('--Insert New Business',@TableListGenerateCode))