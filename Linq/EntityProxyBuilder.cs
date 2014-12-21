using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.CodeDom;
using System.Reflection;
using System.Reflection.Emit;
using TSVCEO.DataModelling;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.Linq.Expressions;

namespace TSVCEO.DataModelling.Linq
{
    public class EntityProxyBuilder
    {
        private ModuleBuilder _ModBuilder;
        protected Dictionary<Type, Type> EntityProxies = new Dictionary<Type, Type>();

        public EntityProxyBuilder(IEnumerable<EntityMap> maps)
        {
            AppDomain appdomain = AppDomain.CurrentDomain;
            AssemblyName asmname = new AssemblyName("EntityProxy_" + Guid.NewGuid().ToString().Substring(0, 8));
            AssemblyBuilder asmbuilder = appdomain.DefineDynamicAssembly(asmname, AssemblyBuilderAccess.Run);
            _ModBuilder = asmbuilder.DefineDynamicModule(asmname + ".dll");

            foreach (EntityMap map in maps)
            {
                BuildProxy(map);
            }
        }

        protected void BuildProxy(EntityMap map)
        {
            TypeBuilder builder = _ModBuilder.DefineType(map.EntityType.Name + "_" + Guid.NewGuid().ToString().Substring(0, 8), TypeAttributes.Public | TypeAttributes.Class, map.EntityType);
            builder.SetCustomAttribute(BuildTableAttribute(map));

            string pkcolname = map.PrimaryKey == null ? null : map.PrimaryKey.KeyColumn.Name;

            foreach (IColumnDef coldef in map.Columns)
            {
                BuildColumnProperty(builder, coldef, pkcolname != null && coldef.Name == pkcolname);
            }

            foreach (IForeignKeyMap fkmap in map.ForeignKeys)
            {
                BuildForeignKeyProperty(builder, fkmap);
            }

            foreach (IManyToOneMap manymap in map.ManyToOneMappings)
            {
                if (manymap.HasMany)
                {
                    BuildManyToOneProperty(builder, manymap);
                }
                else
                {
                    BuildOneToOneProperty(builder, manymap);
                }
            }

            EntityProxies[map.EntityType] = builder.CreateType();
        }

        protected CustomAttributeBuilder BuildTableAttribute(EntityMap map)
        {
            Type attribtype = typeof(TableAttribute);
            ConstructorInfo ctor = attribtype.GetConstructor(new Type[] { });
            PropertyInfo nameprop = attribtype.GetProperty("Name");

            return new CustomAttributeBuilder(
                typeof(TableAttribute).GetConstructor(new Type[] { }),
                new object[] { },
                new PropertyInfo[] { nameprop },
                new object[] { map.TableName }
            );
        }

        protected void BuildColumnProperty(TypeBuilder typebuilder, IColumnDef coldef, bool isprimarykey)
        {
            PropertyInfo basepropinfo = coldef.MemberInfo;

            PropertyBuilder builder = typebuilder.DefineProperty(
                coldef.PropertyName,
                PropertyAttributes.HasDefault,
                coldef.PropertyType,
                null
            );

            MethodAttributes accessorattribs = MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig | MethodAttributes.Virtual;

            MethodBuilder getbuilder = typebuilder.DefineMethod(basepropinfo.GetGetMethod().Name, accessorattribs, coldef.PropertyType, null);
            ILGenerator getilgen = getbuilder.GetILGenerator();
            getilgen.Emit(OpCodes.Ldarg_0);
            getilgen.Emit(OpCodes.Callvirt, basepropinfo.GetGetMethod());
            getilgen.Emit(OpCodes.Ret);
            builder.SetGetMethod(getbuilder);

            MethodBuilder setbuilder = typebuilder.DefineMethod(basepropinfo.GetSetMethod().Name, accessorattribs, null, new Type[] { coldef.PropertyType });
            ILGenerator setilgen = setbuilder.GetILGenerator();
            setilgen.Emit(OpCodes.Ldarg_0);
            setilgen.Emit(OpCodes.Ldarg_1);
            setilgen.Emit(OpCodes.Callvirt, basepropinfo.GetSetMethod());
            getilgen.Emit(OpCodes.Ret);
            builder.SetSetMethod(setbuilder);

            Type attribtype = typeof(ColumnAttribute);
            ConstructorInfo ctor = attribtype.GetConstructor(new Type[] { });
            PropertyInfo nameprop = attribtype.GetProperty("Name");
            PropertyInfo nullableprop = attribtype.GetProperty("CanBeNull");
            PropertyInfo pkprop = attribtype.GetProperty("IsPrimaryKey");

            CustomAttributeBuilder attribbuilder = new CustomAttributeBuilder(
                attribtype.GetConstructor(new Type[] { }),
                new object[] { },
                new PropertyInfo[] { nameprop, nullableprop, pkprop },
                new object[] { coldef.Name, coldef.Type.IsNullable, isprimarykey }
            );

            builder.SetCustomAttribute(attribbuilder);
        }

        protected void BuildForeignKeyProperty(TypeBuilder typebuilder, IForeignKeyMap fkmap)
        {
            PropertyInfo basepropinfo = fkmap.PropertyRef.MemberInfo;

            Type entityreftype = typeof(EntityRef<>).MakeGenericType(fkmap.PropertyRef.PropertyType);

            FieldBuilder fldbuilder = typebuilder.DefineField("_" + basepropinfo.Name, entityreftype, FieldAttributes.Private);

            PropertyBuilder builder = typebuilder.DefineProperty(
                fkmap.PropertyRef.PropertyName,
                PropertyAttributes.HasDefault,
                fkmap.PropertyRef.PropertyType,
                null
            );

            MethodAttributes accessorattribs = MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig | MethodAttributes.Virtual;

            MethodBuilder getbuilder = typebuilder.DefineMethod(basepropinfo.GetGetMethod().Name, accessorattribs, fkmap.PropertyRef.PropertyType, null);
            ILGenerator getilgen = getbuilder.GetILGenerator();
            getilgen.Emit(OpCodes.Ldarg_0);
            getilgen.Emit(OpCodes.Ldfld, fldbuilder);
            getilgen.Emit(OpCodes.Call, entityreftype.GetProperty("Entity").GetGetMethod());
            getilgen.Emit(OpCodes.Ret);
            builder.SetGetMethod(getbuilder);

            MethodBuilder setbuilder = typebuilder.DefineMethod(basepropinfo.GetSetMethod().Name, accessorattribs, null, new Type[] { fkmap.PropertyRef.PropertyType });
            ILGenerator setilgen = setbuilder.GetILGenerator();
            setilgen.Emit(OpCodes.Ldarg_0);
            setilgen.Emit(OpCodes.Ldfld, fldbuilder);
            setilgen.Emit(OpCodes.Ldarg_1);
            setilgen.Emit(OpCodes.Call, entityreftype.GetProperty("Entity").GetSetMethod());
            getilgen.Emit(OpCodes.Ret);
            builder.SetSetMethod(setbuilder);

            Type attribtype = typeof(AssociationAttribute);
            ConstructorInfo ctor = attribtype.GetConstructor(new Type[] { });
            PropertyInfo storageprop = attribtype.GetProperty("Storage");
            PropertyInfo thiskeyprop = attribtype.GetProperty("ThisKey");
            PropertyInfo otherkeyprop = attribtype.GetProperty("OtherKey");

            CustomAttributeBuilder attribbuilder = new CustomAttributeBuilder(
                attribtype.GetConstructor(new Type[] { }),
                new object[] { },
                new PropertyInfo[] { storageprop, thiskeyprop, otherkeyprop },
                new object[] { fldbuilder.Name, fkmap.IdColumn.PropertyName, fkmap.ReferencedKey.Table.IdColumn.PropertyRef.PropertyName }
            );

            builder.SetCustomAttribute(attribbuilder);
        }

        protected void BuildOneToOneProperty(TypeBuilder typebuilder, IManyToOneMap manymap)
        {
            PropertyInfo basepropinfo = manymap.PropertyRef.MemberInfo;

            Type entityreftype = typeof(EntityRef<>).MakeGenericType(manymap.PropertyRef.PropertyType);

            FieldBuilder fldbuilder = typebuilder.DefineField("_" + basepropinfo.Name, entityreftype, FieldAttributes.Private);

            PropertyBuilder builder = typebuilder.DefineProperty(
                manymap.PropertyRef.PropertyName,
                PropertyAttributes.HasDefault,
                manymap.PropertyRef.PropertyType,
                null
            );

            MethodAttributes accessorattribs = MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig | MethodAttributes.Virtual;

            MethodBuilder getbuilder = typebuilder.DefineMethod(basepropinfo.GetGetMethod().Name, accessorattribs, manymap.PropertyRef.PropertyType, null);
            ILGenerator getilgen = getbuilder.GetILGenerator();
            getilgen.Emit(OpCodes.Ldarg_0);
            getilgen.Emit(OpCodes.Ldfld, fldbuilder);
            getilgen.Emit(OpCodes.Call, entityreftype.GetProperty("Entity").GetGetMethod());
            getilgen.Emit(OpCodes.Ret);
            builder.SetGetMethod(getbuilder);

            MethodBuilder setbuilder = typebuilder.DefineMethod(basepropinfo.GetSetMethod().Name, accessorattribs, null, new Type[] { manymap.PropertyRef.PropertyType });
            ILGenerator setilgen = setbuilder.GetILGenerator();
            setilgen.Emit(OpCodes.Ldarg_0);
            setilgen.Emit(OpCodes.Ldfld, fldbuilder);
            setilgen.Emit(OpCodes.Ldarg_1);
            setilgen.Emit(OpCodes.Call, entityreftype.GetProperty("Entity").GetSetMethod());
            getilgen.Emit(OpCodes.Ret);
            builder.SetSetMethod(setbuilder);

            Type attribtype = typeof(AssociationAttribute);
            ConstructorInfo ctor = attribtype.GetConstructor(new Type[] { });
            PropertyInfo storageprop = attribtype.GetProperty("Storage");
            PropertyInfo thiskeyprop = attribtype.GetProperty("ThisKey");
            PropertyInfo otherkeyprop = attribtype.GetProperty("OtherKey");

            CustomAttributeBuilder attribbuilder = new CustomAttributeBuilder(
                attribtype.GetConstructor(new Type[] { }),
                new object[] { },
                new PropertyInfo[] { storageprop, thiskeyprop, otherkeyprop },
                new object[] { fldbuilder.Name, manymap.Table.IdColumn.PropertyRef.PropertyName, manymap.ForeignKey.IdColumn.PropertyName }
            );

            builder.SetCustomAttribute(attribbuilder);
        }

        protected void BuildManyToOneProperty(TypeBuilder typebuilder, IManyToOneMap manymap)
        {
            PropertyInfo basepropinfo = manymap.PropertyRef.MemberInfo;

            Type entitysettype = typeof(EntitySet<>).MakeGenericType(manymap.PropertyRef.PropertyType);

            FieldBuilder fldbuilder = typebuilder.DefineField("_" + basepropinfo.Name, entitysettype, FieldAttributes.Private);

            PropertyBuilder builder = typebuilder.DefineProperty(
                manymap.PropertyRef.PropertyName,
                PropertyAttributes.HasDefault,
                manymap.PropertyRef.PropertyType,
                null
            );

            MethodAttributes accessorattribs = MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig | MethodAttributes.Virtual;

            MethodBuilder getbuilder = typebuilder.DefineMethod(basepropinfo.GetGetMethod().Name, accessorattribs, manymap.PropertyRef.PropertyType, null);
            ILGenerator getilgen = getbuilder.GetILGenerator();
            getilgen.Emit(OpCodes.Ldarg_0);
            getilgen.Emit(OpCodes.Ldfld, fldbuilder);
            getilgen.Emit(OpCodes.Ret);
            builder.SetGetMethod(getbuilder);

            MethodBuilder setbuilder = typebuilder.DefineMethod(basepropinfo.GetSetMethod().Name, accessorattribs, null, new Type[] { manymap.PropertyRef.PropertyType });
            ILGenerator setilgen = setbuilder.GetILGenerator();
            setilgen.Emit(OpCodes.Ldarg_0);
            setilgen.Emit(OpCodes.Ldfld, fldbuilder);
            setilgen.Emit(OpCodes.Ldarg_1);
            setilgen.Emit(OpCodes.Call, entitysettype.GetMethod("Assign"));
            getilgen.Emit(OpCodes.Ret);
            builder.SetSetMethod(setbuilder);

            Type attribtype = typeof(AssociationAttribute);
            ConstructorInfo ctor = attribtype.GetConstructor(new Type[] { });
            PropertyInfo storageprop = attribtype.GetProperty("Storage");
            PropertyInfo thiskeyprop = attribtype.GetProperty("ThisKey");
            PropertyInfo otherkeyprop = attribtype.GetProperty("OtherKey");

            CustomAttributeBuilder attribbuilder = new CustomAttributeBuilder(
                attribtype.GetConstructor(new Type[] { }),
                new object[] { },
                new PropertyInfo[] { storageprop, thiskeyprop, otherkeyprop },
                new object[] { fldbuilder.Name, manymap.Table.IdColumn.PropertyRef.PropertyName, manymap.ForeignKey.IdColumn.PropertyName }
            );

            builder.SetCustomAttribute(attribbuilder);
        }    
    }
}
