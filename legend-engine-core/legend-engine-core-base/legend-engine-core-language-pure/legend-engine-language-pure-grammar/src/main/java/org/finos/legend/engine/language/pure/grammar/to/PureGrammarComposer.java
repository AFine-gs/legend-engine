// Copyright 2020 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.engine.language.pure.grammar.to;

import org.eclipse.collections.api.block.function.Function3;
import org.eclipse.collections.api.block.predicate.Predicate;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.block.factory.Predicates;
import org.eclipse.collections.impl.utility.LazyIterate;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.language.pure.grammar.from.connection.ConnectionParser;
import org.finos.legend.engine.language.pure.grammar.from.domain.DomainParser;
import org.finos.legend.engine.language.pure.grammar.from.mapping.MappingParser;
import org.finos.legend.engine.language.pure.grammar.from.runtime.RuntimeParser;
import org.finos.legend.engine.language.pure.grammar.to.extension.PureGrammarComposerExtension;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.protocol.pure.m3.PackageableElement;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.connection.PackageableConnection;
import org.finos.legend.engine.protocol.pure.m3.relationship.Association;
import org.finos.legend.engine.protocol.pure.m3.type.Class;
import org.finos.legend.engine.protocol.pure.m3.type.Enumeration;
import org.finos.legend.engine.protocol.pure.m3.function.Function;
import org.finos.legend.engine.protocol.pure.m3.type.Measure;
import org.finos.legend.engine.protocol.pure.m3.extension.Profile;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.mapping.Mapping;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.runtime.PackageableRuntime;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.section.ImportAwareCodeSection;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.section.Section;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.section.SectionIndex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PureGrammarComposer
{
    private static final String DEFAULT_SECTION_NAME = "Pure";
    private final PureGrammarComposerContext context;

    private PureGrammarComposer(PureGrammarComposerContext context)
    {
        this.context = context;
    }

    public static PureGrammarComposer newInstance(PureGrammarComposerContext context)
    {
        return new PureGrammarComposer(context);
    }

    public String renderPureModelContextData(PureModelContextData pureModelContextData)
    {
        return renderPureModelContextData(pureModelContextData, null, false);
    }

    public String renderPureModelContextData(PureModelContextData pureModelContextData, boolean isPureGrammar)
    {
        return renderPureModelContextData(pureModelContextData, null, isPureGrammar);
    }

    public String renderPureModelContextData(PureModelContextData pureModelContextData, org.eclipse.collections.api.block.function.Function<PackageableElement, String> comparator)
    {
        return renderPureModelContextData(pureModelContextData, comparator, false);
    }

    public String renderPureModelContextData(PureModelContextData pureModelContextData, org.eclipse.collections.api.block.function.Function<PackageableElement, String> comparator, boolean isPureGrammar)
    {
        List<PackageableElement> elements = pureModelContextData.getElements();
        Set<PackageableElement> elementsToCompose = new HashSet<>(elements);
        MutableList<String> composedSections = Lists.mutable.empty();
        if (ListIterate.anySatisfy(elements, e -> e instanceof SectionIndex))
        {
            Map<String, PackageableElement> elementByPath = new HashMap<>();
            // NOTE: here we handle duplication, first element with the duplicated path wins
            elements.forEach(element -> elementByPath.putIfAbsent(element.getPath(), element));
            LazyIterate.selectInstancesOf(elements, SectionIndex.class).forEach(sectionIndex -> this.renderSectionIndex(sectionIndex, elementByPath, elementsToCompose, composedSections, comparator, isPureGrammar));
        }

        for (Function3<List<PackageableElement>, PureGrammarComposerContext, List<String>, PureGrammarComposerExtension.PureFreeSectionGrammarComposerResult> composer : this.context.extraFreeSectionComposers)
        {
            PureGrammarComposerExtension.PureFreeSectionGrammarComposerResult result = composer.value(ListIterate.select(elements, elementsToCompose::contains), this.context, composedSections);
            if (result != null)
            {
                composedSections.add(result.value + "\n");
                // mark that the elements already been rendered by one of the extensions
                result.composedElements.forEach(elementsToCompose::remove);
            }
        }

        Predicate<PackageableElement> isDomainElement = e ->
                (e instanceof Class) ||
                        (e instanceof Association) ||
                        (e instanceof Enumeration) ||
                        (e instanceof Function) ||
                        (e instanceof Profile) ||
                        (e instanceof Measure);
        if (ListIterate.anySatisfy(elements, isDomainElement))
        {
            this.DEPRECATED_renderSection(DomainParser.name, comparator == null ? ListIterate.select(elements, isDomainElement) : ListIterate.select(elements, isDomainElement).sortThisBy(comparator), elementsToCompose, composedSections);
        }
        this.DEPRECATED_renderSection(MappingParser.name, comparator == null ? pureModelContextData.getElementsOfType(Mapping.class) : pureModelContextData.getElementsOfType(Mapping.class).sortThisBy(comparator), elementsToCompose, composedSections);
        this.DEPRECATED_renderSection(ConnectionParser.name, comparator == null ? pureModelContextData.getElementsOfType(PackageableConnection.class) : pureModelContextData.getElementsOfType(PackageableConnection.class).sortThisBy(comparator), elementsToCompose, composedSections);
        this.DEPRECATED_renderSection(RuntimeParser.name, comparator == null ? pureModelContextData.getElementsOfType(PackageableRuntime.class) : pureModelContextData.getElementsOfType(PackageableRuntime.class).sortThisBy(comparator), elementsToCompose, composedSections);
        return composedSections.select(section -> !section.isEmpty()).makeString("\n\n");
    }

    private void DEPRECATED_renderSection(String parserName, List<? extends PackageableElement> elements, Set<PackageableElement> elementsToCompose, List<String> composedSections)
    {
        List<? extends PackageableElement> els = ListIterate.select(elements, elementsToCompose::contains);
        els.forEach(elementsToCompose::remove);
        if (!els.isEmpty())
        {
            StringBuilder builder = new StringBuilder();
            builder.append(composedSections.size() > 0 || !parserName.equals(DEFAULT_SECTION_NAME) ? ("###" + parserName + "\n") : "");
            builder.append(LazyIterate.collect(els, this::DEPRECATED_renderElement).makeString("\n\n"));
            builder.append("\n");
            composedSections.add(builder.toString());
        }
    }

    private void renderSectionIndex(SectionIndex sectionIndex, Map<String, PackageableElement> elementByPath, Set<PackageableElement> elementsToCompose, List<String> composedSections, org.eclipse.collections.api.block.function.Function<PackageableElement, String> comparator)
    {
        renderSectionIndex(sectionIndex, elementByPath, elementsToCompose, composedSections, comparator, false);
    }

    private void renderSectionIndex(SectionIndex sectionIndex, Map<String, PackageableElement> elementByPath, Set<PackageableElement> elementsToCompose, List<String> composedSections, org.eclipse.collections.api.block.function.Function<PackageableElement, String> comparator, boolean isPureGrammar)
    {
        List<Section> sections = sectionIndex.sections;
        ListIterate.forEach(sections, section ->
        {
            StringBuilder builder = new StringBuilder();
            builder.append(composedSections.size() > 0 || !section.parserName.equals(DEFAULT_SECTION_NAME) ? ("###" + section.parserName + "\n") : "");
            // NOTE: here we remove duplicates in both the imports and the content
            List<String> imports = section instanceof ImportAwareCodeSection ? ListIterate.distinct(((ImportAwareCodeSection) section).imports) : new ArrayList<>();
            if (!imports.isEmpty())
            {
                builder.append(LazyIterate.collect(imports, _import -> ("import " + PureGrammarComposerUtility.convertPath(_import) + "::*;")).makeString("\n"));
                builder.append("\n");
            }
            MutableList<PackageableElement> _elements = ListIterate.distinct(section.elements).collect(path ->
            {
                PackageableElement element = elementByPath.get(path);
                // mark that the elements already been rendered in one of the section
                elementsToCompose.remove(element);
                return element;
            }).select(Objects::nonNull);
            MutableList<PackageableElement> elements = comparator == null ? _elements : _elements.sortThisBy(comparator);
            if (!elements.isEmpty())
            {
                builder.append(this.context.extraSectionComposers.stream().map(composer -> composer.value(elements, this.context, section.parserName)).filter(Objects::nonNull).findFirst()
                        // NOTE: this is the old way (no-plugin) way to render section elements, this approach is not great since it does not enforce
                        // the types of elements a section can have, the newer approach does the check and compose unsupported message when such violations occur
                        // TO BE REMOVED when we moved everything to extensions
                        .orElseGet(() -> LazyIterate.collect(elements, e -> DEPRECATED_renderElement(e, isPureGrammar)).makeString("\n\n")));
                builder.append("\n");
            }
            composedSections.add(builder.toString());
        });
    }

    private String DEPRECATED_renderElement(PackageableElement element)
    {
        return DEPRECATED_renderElement(element, false);
    }

    private String DEPRECATED_renderElement(PackageableElement element, boolean isPureGrammar)
    {
        return (isPureGrammar ? element.accept(DEPRECATED_PureGrammarComposerCore.Builder.newInstance(this.context).withPureGrammar().build())
                : element.accept(DEPRECATED_PureGrammarComposerCore.Builder.newInstance(this.context).build()));
    }

    public String render(PackageableElement element)
    {
        return DEPRECATED_renderElement(element);
    }

    public String render(PackageableElement element, String parser)
    {
        return this.context.extraSectionComposers.stream().map(composer -> composer.value(Lists.mutable.with(element), this.context, parser)).filter(Objects::nonNull).findFirst()
                // NOTE: this is the old way (no-plugin) way to render section elements, this approach is not great since it does not enforce
                // the types of elements a section can have, the newer approach does the check and compose unsupported message when such violations occur
                // TO BE REMOVED when we moved everything to extensions
                .orElseGet(() -> LazyIterate.collect(Lists.mutable.with(element), this::DEPRECATED_renderElement).makeString("\n\n"));
    }

    static String processReturn(PackageableElement element, MutableList<String> select)
    {
        if (select.size() == 1)
        {
            return select.getFirst();
        }
        else if (select.isEmpty())
        {
            return "/* Can't transform element '" + element.getPath() + "' in this section */";
        }
        else
        {
            return "Found " + select.size() + " composers for the Element " + element.getPath();
        }
    }

    public static Function3<List<PackageableElement>, PureGrammarComposerContext, String, String> buildSectionComposer(String theSectionName, MutableList<org.eclipse.collections.api.block.function.Function2<PackageableElement, PureGrammarComposerContext, String>> renderers)
    {
        return (elements, context, sectionName) ->
        {
            if (!theSectionName.equals(sectionName))
            {
                return null;
            }
            return ListIterate.collect(elements, element -> processReturn(element, renderers.collect(r -> r.apply(element, context)).select(Predicates.notNull()))).makeString("\n\n");
        };
    }
}
